"""Main module."""

from iomete_jdbc_sync import DataSyncer
from sync.iomete_logger import init_logger


def start_job(spark, config):
    init_logger()
    data_syncer = DataSyncer(spark, config)
    data_syncer.run()
