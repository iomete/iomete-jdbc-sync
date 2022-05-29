import logging

from ._lakehouse import Lakehouse
from .sync_mode import SyncMode, FullLoad, IncrementalSnapshot

logger = logging.getLogger(__name__)


class DataSyncFactory:
    @staticmethod
    def instance_for(sync_mode: SyncMode, lakehouse: Lakehouse):
        if isinstance(sync_mode, FullLoad):
            return FullLoadDataSync(lakehouse)

        if isinstance(sync_mode, IncrementalSnapshot):
            return IncrementalSnapshotDataSync(lakehouse, sync_mode)

        raise Exception(f"Unsupported load type: {sync_mode}")


class DataSync:
    def sync(self, proxy_table_name, staging_table_name):
        pass


class IncrementalSnapshotDataSync(DataSync):

    def __init__(self, lakehouse: Lakehouse, incremental_load_settings: IncrementalSnapshot):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.lakehouse = lakehouse
        self.incremental_load_settings = incremental_load_settings
        self.full_load_table_recreate_data_migration = FullLoadDataSync(lakehouse)

    def sync(self, proxy_table_name, staging_table_name):
        logger.debug("sync proxy_table_name: %s, staging_table_name: %s", proxy_table_name, staging_table_name)
        if not self.lakehouse.table_exists(staging_table_name):
            self.logger.info("Table doesn't exists. Doing full dump")
            self.full_load_table_recreate_data_migration.sync(proxy_table_name, staging_table_name)
            return

        id_column_name = self.incremental_load_settings.identification_column
        tracking_column = self.incremental_load_settings.tracking_column

        max_tracking_value = self.lakehouse.query_single_value(
            query=f"select max({tracking_column}) from {staging_table_name}")

        proxy_view_column_names = self.lakehouse.table_column_names(proxy_table_name)

        insert_columns = ",".join(proxy_view_column_names)
        insert_values = ",".join(f"src.{c}" for c in proxy_view_column_names)

        update_columns = ",".join([f"trg.{c}=src.{c}"
                                   for c in proxy_view_column_names if c != id_column_name])
        merge_query = f"""
                MERGE INTO {staging_table_name} trg
                USING (SELECT * FROM {proxy_table_name} 
                        WHERE {tracking_column} > '{max_tracking_value}') src
                ON (src.{id_column_name} = trg.{id_column_name})
                WHEN MATCHED THEN 
                    UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN 
                    INSERT ({insert_columns}) VALUES({insert_values})
            """

        result = self.lakehouse.execute(merge_query)

        logger.debug("sync finished! result: %s", result)


class FullLoadDataSync(DataSync):
    def __init__(self, lakehouse: Lakehouse):
        self.lakehouse = lakehouse

    def sync(self, proxy_table_name, staging_table_name):
        logger.debug("Sync started..., proxy_table_name: %s, staging_table_name: %s",
                     proxy_table_name, staging_table_name)

        result = self.lakehouse.execute(
            f"CREATE OR REPLACE TABLE {staging_table_name} SELECT * FROM {proxy_table_name}")
        logger.debug("sync finished!")

        return result
