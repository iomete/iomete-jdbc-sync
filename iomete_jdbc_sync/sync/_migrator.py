import logging
import time

from ._lakehouse import Lakehouse
from ._sync_strategy import DataSyncFactory
from .config import ApplicationConfig, SyncConfig, Table

logger = logging.getLogger(__name__)


class DataSyncer:
    def __init__(self, spark, config: ApplicationConfig):
        self.spark = spark
        self.source_connection = config.source_connection
        self.sync_configs = config.sync_configs

    def run(self):
        timer("Data sync")(self._run_internal)()

    def _run_internal(self):
        logger.info(f"Connection={str(self.source_connection)}")
        for sync_config in self.sync_configs:
            message = f"Syncing source schema '{sync_config.source.schema}' with mode: {sync_config.sync_mode}"

            migrator = SyncSingleConfig(self.spark, self.source_connection, sync_config)
            timer(message)(migrator.sync_tables)()


class SyncSingleConfig:
    def __init__(self, spark, source_connection, sync_config: SyncConfig):
        self.source_connection = source_connection
        self.sync_config = sync_config
        self.drop_proxy_table_after_migration = True

        self.lakehouse = Lakehouse(spark=spark, db_name=sync_config.destination.schema)

    def sync_tables(self):
        tables = self.sync_config.source.tables
        if self.sync_config.source.is_all_tables:
            tables = self.__get_tables_of_source_database()

        exclude_tables = set(self.sync_config.source.exclude_tables or [])

        tables = [table for table in tables if table.name not in exclude_tables]

        self.__log_tables(tables)

        max_table_name_length = max([len(table.name) for table in tables])

        for table in tables:
            message = f"[{table.name: <{max_table_name_length}}]: table sync"
            timer(message)(self.__sync_table)(table)

    def __get_tables_of_source_database(self):
        information_tables_proxy_name = "information_tables_proxy"
        self.lakehouse.execute(
            self.source_connection.proxy_table_definition_for_info_schema(information_tables_proxy_name))

        source_tables = self.lakehouse.execute(f"""
                    select * from {information_tables_proxy_name} 
                        where TABLE_SCHEMA = '{self.sync_config.source.schema}'""")

        tables = [Table(name=tbl.TABLE_NAME, definition=tbl.TABLE_NAME) for tbl in source_tables]

        self.lakehouse.execute(f"drop table if exists {information_tables_proxy_name}")

        return tables

    @staticmethod
    def __log_tables(tables):
        new_line_tab = "\n\t- "
        log_tables = new_line_tab.join([table.name for table in tables])
        logger.info(f"Following tables will be synced: {new_line_tab}{log_tables}")

    def __sync_table(self, table: Table):
        proxy_table_name = self.lakehouse.proxy_table_name(table.name)
        staging_table_name = self.lakehouse.staging_table_name(table.name)

        self.__create_proxy_table(source_table=table.quoted_definition(), proxy_table_name=proxy_table_name)

        data_sync = DataSyncFactory.instance_for(
            sync_mode=self.sync_config.sync_mode, lakehouse=self.lakehouse
        )

        data_sync.sync(proxy_table_name, staging_table_name)

        if self.drop_proxy_table_after_migration:
            logger.debug(f"Cleaning up proxy table: {proxy_table_name}")
            self.lakehouse.execute(f"DROP TABLE {proxy_table_name}")

    def __create_proxy_table(self, source_table: str, proxy_table_name):
        self.lakehouse.create_database_if_not_exists()

        self.lakehouse.execute(
            self.source_connection.proxy_table_definition(
                source_schema=self.sync_config.source.schema,
                source_table=source_table,
                proxy_table_name=proxy_table_name))


def timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"{message} started")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator
