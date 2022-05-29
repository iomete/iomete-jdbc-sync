import logging
import time

from ._lakehouse import Lakehouse
from ._sync_strategy import DataSyncFactory
from .config import ApplicationConfig, SyncConfig

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
        self.__log_tables()

        max_table_name_length = max([len(table_name) for table_name in self.sync_config.source.tables])

        for table_name in self.sync_config.source.tables:
            message = f"[{table_name: <{max_table_name_length}}]: table sync"
            timer(message)(self.__sync_table)(table_name)

    def __log_tables(self):
        new_line_tab = "\n\t"
        log_tables = new_line_tab.join([table for table in self.sync_config.source.tables])
        logger.info(f"Following tables will be synced: {new_line_tab}{log_tables}")

    def __sync_table(self, table_name: str):
        proxy_table_name = self.lakehouse.proxy_table_name(table_name)
        staging_table_name = self.lakehouse.staging_table_name(table_name)

        self.__create_proxy_table(table_name, proxy_table_name)

        data_sync = DataSyncFactory.instance_for(
            sync_mode=self.sync_config.sync_mode, lakehouse=self.lakehouse
        )

        data_sync.sync(proxy_table_name, staging_table_name)

        if self.drop_proxy_table_after_migration:
            logger.debug(f"Cleaning up proxy table: {proxy_table_name}")
            self.lakehouse.execute(f"DROP TABLE {proxy_table_name}")

    def __create_proxy_table(self, table_name: str, proxy_table_name):
        self.lakehouse.create_database_if_not_exists()

        self.lakehouse.execute(
            self.source_connection.proxy_table_definition(
                source_schema=self.sync_config.source.schema,
                source_table=table_name,
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
