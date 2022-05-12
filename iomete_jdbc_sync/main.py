"""Main module."""

from iomete_jdbc_sync import DataSyncer


def start_job(spark, config):
    data_syncer = DataSyncer(spark, config)
    data_syncer.run()
