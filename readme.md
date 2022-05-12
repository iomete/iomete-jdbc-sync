# iomete: JDBC Sync

This library provides easily replicate tables from JDBC databases (MySQL, PostgreSQL, etc.) to iomete

> Note: It requires you have SSH Tunnel between iomete if your database in a private network (see: https://docs.iomete.com/docs/database-connection-options)

## Sync mode

You can define sync mode for each table. Currently, supported sync modes are:

- `FullLoad`: Read everything in the source and overwrites whole table at the destination at each sync
- `IncrementalSnapshot`: It creates the snapshot of table in the destination and only move the newly inserted and
  updated records. While writing to iomete it uses merge statement. This mode requires 2
  parameters: `identification_column` will be used on merge statement, and `tracking_column` to track the where it
  should continue to get data from the source table


### Configuration example

```hocon
{
  "source_connection": {
    "type": "mysql",
    "host": "iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com",
    "port": "3306",
    "username": "tutorial_user",
    "password": ${MYSQL_PASSWORD}
  },
  "source_schema": "employees",
  "destination_schema": "employees_raw",
  "syncs": [
    {
      "table_names": ["table1", "table2"],
      "sync_mode": {
        "type": "full_load"
      }
    },
    {
      "table_names": ["table3", "table4"],
      "sync_mode": {
        "type": "incremental_snapshot",
        "identification_column": "id",
        "tracking_column": "updated_at"
      }
    }
  ]
}
```

## Development

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

**Run test**

```shell
pytest
```