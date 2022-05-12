class SourceConnection:
    def __init__(self, host: str, port: str, schema: str, user_name: str, user_pass: str):
        self.host = host
        self.port = port
        self.schema = schema
        self.user_name = user_name
        self.user_pass = user_pass

    @property
    def jdbc_url(self):
        return None

    @property
    def jdbc_driver(self):
        return None

    def proxy_table_definition(self, table_name, proxy_table_name):
        return f"""
            CREATE TABLE IF NOT EXISTS {proxy_table_name}
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              url '{self.jdbc_url}',
              dbtable '{self.schema}.{table_name}',
              user '{self.user_name}',
              password '{self.user_pass}',
              driver '{self.jdbc_driver}',
              sessionInitStatement "SET SESSION time_zone='+00:00'"
            )
        """


class MySQLConnection(SourceConnection):
    def __init__(self, host: str, port: str, schema: str, user_name: str, user_pass: str):
        super().__init__(host, port, schema, user_name, user_pass)

    @property
    def jdbc_url(self):
        return f'jdbc:mysql://{self.host}:{self.port}/{self.schema}?zeroDateTimeBehavior=convertToNull'

    @property
    def jdbc_driver(self):
        return 'com.mysql.jdbc.Driver'

    def __str__(self):
        return f"MySQLConnection(host: '{self.host}', port: '{self.port}', schema: '{self.schema}', user_name: '{self.user_name}')"


class PostgreSQLConnection(SourceConnection):
    def __init__(self, host: str, port: str, schema: str, user_name: str, user_pass: str):
        super().__init__(host, port, schema, user_name, user_pass)

    @property
    def jdbc_url(self):
        return f'jdbc:postgresql://{self.host}:{self.port}/{self.schema}'

    @property
    def jdbc_driver(self):
        return 'org.postgresql.Driver'

    def __str__(self):
        return f"PostgreSQLConnection(host: '{self.host}', port: '{self.port}', schema: '{self.schema}', user_name: '{self.user_name}')"
