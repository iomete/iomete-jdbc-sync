from dataclasses import dataclass
from typing import List

from iomete_jdbc_sync.connection.source import MySQLConnection

from iomete_jdbc_sync.sync.sync_mode import SyncMode, FullLoad, IncrementalSnapshot
from pyhocon import ConfigFactory


@dataclass
class SyncSource:
    schema: str
    tables: List[str]
    is_all_tables: bool = False
    exclude_tables: List[str] = None


@dataclass
class SyncDestination:
    schema: str


@dataclass
class SyncConfig:
    source: SyncSource
    destination: SyncDestination
    sync_mode: SyncMode


@dataclass
class ApplicationConfig:
    source_connection: MySQLConnection
    sync_configs: List[SyncConfig]


def get_config(application_config_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_config_path)

    source_connection = None
    if config['source_connection']['type'] == 'mysql':
        source_connection = MySQLConnection(
            host=config['source_connection']['host'],
            port=config['source_connection']['port'],
            user_name=config['source_connection']['username'],
            user_pass=config['source_connection']['password']
        )

    def parse_sync_config(sync_config):
        sync_mode = None
        if sync_config["sync_mode"]["type"] == "full_load":
            sync_mode = FullLoad()
        elif sync_config["sync_mode"]["type"] == "incremental_snapshot":
            sync_mode = IncrementalSnapshot(
                sync_config["sync_mode"]["identification_column"],
                sync_config["sync_mode"]["tracking_column"]
            )
        else:
            raise Exception("Unknown sync mode {}, allowed sync modes are: {}".format(
                sync_config["sync_mode"]["type"], ["full_load", "incremental_snapshot"]))

        source_tables = sync_config["source"]["tables"]
        return SyncConfig(
            source=SyncSource(
                schema=sync_config["source"]["schema"],
                tables=source_tables,
                is_all_tables=source_tables and source_tables[0] == "*",
                exclude_tables=sync_config.get("source.exclude_tables", [])
            ),
            destination=SyncDestination(sync_config["destination"]["schema"]),
            sync_mode=sync_mode
        )

    return ApplicationConfig(
        source_connection=source_connection,
        sync_configs=[parse_sync_config(sync_config) for sync_config in config["syncs"] or []]
    )
