from iomete_jdbc_sync.main import start_job
from iomete_jdbc_sync.sync.config import get_config
from iomete_jdbc_sync.test._spark_session import get_spark_session

from sync.iomete_logger import init_logger


def test_table_migration():
    # create test spark instance
    test_config = get_config("application.conf")
    # spark = get_spark_session()
    spark = get_spark_session()

    # run target
    start_job(spark, test_config)

    # check
    departmentsDf = spark.sql("select * from employees_raw.departments")
    departmentsDf.show()
    assert departmentsDf.count() > 0


if __name__ == '__main__':
    test_table_migration()
