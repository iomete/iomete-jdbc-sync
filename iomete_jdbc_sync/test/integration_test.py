from iomete_jdbc_sync.main import start_job
from iomete_jdbc_sync.sync.config import get_config
from iomete_jdbc_sync.test._spark_session import get_spark_session


def test_table_migration():
    # create test spark instance
    test_config = get_config("application.conf")

    spark = get_spark_session()
    spark.sql("DROP DATABASE IF EXISTS employees_raw CASCADE")

    # run target
    start_job(spark, test_config)

    # check
    assert spark.sql("select * from employees_raw.employees").count() == 300024
    assert spark.sql("select * from employees_raw.departments").count() == 9
    assert spark.sql("select * from employees_raw.dept_manager").count() == 24
    assert spark.sql("select * from employees_raw.dept_emp").count() == 331603
    assert spark.sql("select * from employees_raw.titles").count() == 443308
    assert spark.sql("select * from employees_raw.total_salaries").count() == 300024


if __name__ == '__main__':
    test_table_migration()
