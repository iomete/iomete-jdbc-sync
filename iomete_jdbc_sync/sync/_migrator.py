import logging
import time

from ._lakehouse import Lakehouse
from ._sync_strategy import DataSyncFactory
from .sync_mode import SyncMode

logger = logging.getLogger(__name__)


class TableConfig:
    def __init__(self, table_name: str, sync_mode: SyncMode):
        self.table_name = table_name
        self.sync_mode = sync_mode

    def __str__(self):
        return f"Table(table_name='{self.table_name}', sync_mode={self.sync_mode})"


class DataSyncer:
    def __init__(self, spark, config):

        self.lakehouse = Lakehouse(spark=spark, db_name=config.destination_schema)
        self.source_connection = config.source_connection
        self.sync_configs = config.sync_configs
        self.drop_proxy_table_after_migration = True

    def run(self):
        logger.info(f"Data sync started for connection={str(self.source_connection)}")
        start_time = time.time()

        for sync_config in self.sync_configs:
            for table_name in sync_config.table_names:
                self.__migrate_table(table_name, sync_config.sync_mode)
        
        end_time = time.time()
        logger.info(f"Data sync completed for connection={str(self.source_connection)} in {end_time - start_time:0.2f} seconds")

    def __migrate_table(self, table_name: str, sync_mode: SyncMode):
        logger.info(f"{table_name} table: sync started with mode={sync_mode}")
        start_time = time.time()
        
        proxy_table_name = self.lakehouse.proxy_table(table_name)
        staging_table_name = self.lakehouse.staging_table_name(table_name)

        self.__create_proxy_table(table_name, proxy_table_name)

        data_sync = DataSyncFactory.instance_for(
            sync_mode=sync_mode, lakehouse=self.lakehouse
        )

        data_sync.sync(proxy_table_name, staging_table_name)

        if self.drop_proxy_table_after_migration:
            logger.debug(f"Cleaning up proxy table: {proxy_table_name}")
            self.lakehouse.execute(f"DROP TABLE {proxy_table_name}")

        end_time = time.time()
        logger.info(f"{table_name} table: completed in {end_time - start_time:0.2f} seconds")

    def __create_proxy_table(self, table_name: str, proxy_table_name):
        self.lakehouse.create_database_if_not_exists()

        self.lakehouse.execute(
            self.source_connection.proxy_table_definition(
                table_name=table_name,
                proxy_table_name=proxy_table_name))
