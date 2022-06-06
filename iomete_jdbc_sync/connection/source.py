import uuid


class SourceConnection:
    def __init__(self, host: str, port: str, user_name: str, user_pass: str):
        self.host = host
        self.port = port
        self.user_name = user_name
        self.user_pass = user_pass

    def jdbc_url(self, schema):
        return None

    @property
    def jdbc_driver(self):
        return None

    def proxy_table_definition_for_info_schema(self):
        return self.proxy_table_definition(
            source_schema="information_schema",
            source_table="TABLES"
        )

    def proxy_table_definition(self, source_schema, source_table):
        clean_uuid = str(uuid.uuid1()).replace("-", "_")
        proxy_table_name = f"___proxy_{clean_uuid}"
        return proxy_table_name, f"""
            CREATE TEMPORARY TABLE {proxy_table_name}
            USING jdbc
            OPTIONS (
              url '{self.jdbc_url(source_schema)}',
              dbtable '{source_table}',
              user '{self.user_name}',
              password '{self.user_pass}',
              driver '{self.jdbc_driver}',
              sessionInitStatement "SET SESSION time_zone='+00:00'"
            )
        """


class MySQLConnection(SourceConnection):
    def __init__(self, host: str, port: str, user_name: str, user_pass: str):
        super().__init__(host, port, user_name, user_pass)

    def jdbc_url(self, schema):
        return f'jdbc:mysql://{self.host}:{self.port}/{schema}?zeroDateTimeBehavior=convertToNull'

    @property
    def jdbc_driver(self):
        return 'com.mysql.cj.jdbc.Driver'

    def __str__(self):
        return f"MySQLConnection(host: '{self.host}')"


class PostgreSQLConnection(SourceConnection):
    def __init__(self, host: str, port: str, user_name: str, user_pass: str):
        super().__init__(host, port, user_name, user_pass)

    def jdbc_url(self, schema):
        return f'jdbc:postgresql://{self.host}:{self.port}/{schema}'

    @property
    def jdbc_driver(self):
        return 'org.postgresql.Driver'

    def __str__(self):
        return f"PostgreSQLConnection(host: '{self.host}')"
