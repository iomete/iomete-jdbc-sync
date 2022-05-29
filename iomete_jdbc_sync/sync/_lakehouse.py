import logging
import re

logger = logging.getLogger(__name__)


class Lakehouse:
    def __init__(self, spark, db_name: str):
        self.spark = spark
        self.db_name = db_name

    def proxy_table_name(self, table_name: str):
        return f"{self.db_name}.__{table_name}_proxy"

    def staging_table_name(self, table_name: str):
        return f"{self.db_name}.{table_name}"

    def table_exists(self, full_table_name: str):
        df = self.spark.sql(f"SHOW TABLES IN {self.db_name}")
        table_part = full_table_name.split(".")[1]
        return df.where(df.tableName == table_part).count() > 0

    def table_column_names(self, full_table_name):
        return self.spark.table(full_table_name).schema.names

    def execute(self, query):
        logger.debug("Executing query: %s", self.__safe_string_for_log(query))
        return self.spark.sql(query).collect()

    def create_database_if_not_exists(self):
        self.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")

    def query_single_value(self, query):
        result = self.execute(query)
        if result and len(result) > 0:
            return result[0][0]
        return None

    @staticmethod
    def __safe_string_for_log(log_text: str):
        log_text = re.sub(r"""(password\s')(.*)(',)""", r"\1*****\3", log_text)
        return log_text
