from iomete_jdbc_sync.main import start_job
from iomete_jdbc_sync.sync.config import get_config
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JDBC migration") \
    .getOrCreate()

production_config = get_config("/etc/configs/application.conf")

start_job(spark, production_config)
