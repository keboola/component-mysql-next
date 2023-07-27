import base64
import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import dataconf
from pyhocon import ConfigTree

KEY_INCLUDE_SCHEMA_NAME = 'include_schema_name'


class ConfigurationBase:

    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING and f.default_factory == dataclasses.MISSING]


@dataclass
class SSHConfiguration(ConfigurationBase):
    host: Optional[str] = None
    username: Optional[str] = None
    pswd_private_key: Optional[str] = None
    pswd_key_password: Optional[str] = None
    port: int = 22

    LOCAL_BIND_ADDRESS = "localhost"
    LOCAL_BIND_PORT = 9800


@dataclass
class SSLConfiguration(ConfigurationBase):
    verifyCert: bool = True


@dataclass
class DbAdvancedParameters(ConfigurationBase):
    max_execution_time: Optional[str] = ""


@dataclass
class DbOptions(ConfigurationBase):
    host: str
    port: int
    user: str
    pswd_password: str
    use_ssh: bool = False
    ssh_options: SSHConfiguration = dataclasses.field(default_factory=lambda: ConfigTree({}))
    use_ssl: bool = False
    ssl_options: SSLConfiguration = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class ShowLogConfig(ConfigurationBase):
    method: str = 'direct'
    endpoint_url: str = ''
    authentication: bool = False
    user: str = ''
    pswd_password: str = ''


@dataclass
class SourceSettings(ConfigurationBase):
    schemas: list[str] = dataclasses.field(default_factory=list)
    tables: list[str] = dataclasses.field(default_factory=list)


class SnapshotMode(str, Enum):
    snapshot_only = "snapshot_only"
    initial = "initial"


class BinaryHandler(str, Enum):
    plain = "plain"
    hex = "hex"
    base64 = "base64"


@dataclass
class SyncOptions(ConfigurationBase):
    snapshot_mode: SnapshotMode = SnapshotMode.initial
    handle_binary: BinaryHandler = BinaryHandler.plain
    show_binary_log_config: ShowLogConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"
    append_only = "append_only"


@dataclass
class DestinationSettings(ConfigurationBase):
    load_type: LoadType = LoadType.incremental_load
    include_schema_name: bool = True
    output_bucket: str = ''

    @property
    def is_incremental_load(self) -> bool:
        return self.load_type == LoadType.incremental_load


@dataclass
class Configuration(ConfigurationBase):
    # Connection options
    db_settings: DbOptions
    advanced_options: DbAdvancedParameters = dataclasses.field(default_factory=lambda: ConfigTree({}))
    source_settings: SourceSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    sync_options: SyncOptions = dataclasses.field(default_factory=lambda: ConfigTree())
    destination: DestinationSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    debug: bool = False


# #### LEGACY CONFIG

KEY_OBJECTS_ONLY = 'fetchObjectsOnly'
KEY_TABLE_MAPPINGS_JSON = 'inputMappingsJson'
KEY_DATABASES = 'databases'
KEY_MYSQL_HOST = 'host'
KEY_MYSQL_PORT = 'port'
KEY_MYSQL_USER = 'username'
KEY_MYSQL_PWD = '#password'
KEY_INCREMENTAL_SYNC = 'runIncrementalSync'
KEY_OUTPUT_BUCKET = 'outputBucket'
KEY_USE_SSH_TUNNEL = 'sshTunnel'
KEY_USE_SSL = 'ssl'
KEY_MAPPINGS_FILE = 'storageMappingsFile'
KEY_APPEND_MODE = 'appendMode'
KEY_HANDLE_BINARY = 'handle_binary'

# Define optional parameters as constants for later use.
KEY_SSH_HOST = 'sshHost'
KEY_SSH_PORT = 'sshPort'
KEY_SSH_PUBLIC_KEY = 'sshPublicKey'
KEY_SSH_PRIVATE_KEY = '#sshBase64PrivateKey'
KEY_SSH_USERNAME = 'sshUser'
KEY_SSL_CA = 'sslCa'
KEY_VERIFY_CERT = 'verifyCert'
KEY_MAX_EXECUTION_TIME = 'maxExecutionTime'

ENV_COMPONENT_ID = 'KBC_COMPONENTID'
ENV_CONFIGURATION_ID = 'KBC_CONFIGID'

MAPPINGS_FILE = 'table_mappings.json'
LOCAL_ADDRESS = '127.0.0.1'

MANDATORY_PARS = [KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD,
                  KEY_USE_SSH_TUNNEL, KEY_USE_SSL]


def get_required_parameters(legacy_config=True) -> list[str]:
    if legacy_config:
        return MANDATORY_PARS
    else:
        return Configuration.get_dataclass_required_parameters()


def is_legacy_config(parameters: dict) -> bool:
    return any([key for key in parameters if key in ["fetchObjectsOnly", "inputMappingsJson"]])


legacy_example = {
    "port": 3306,
    "fetchObjectsOnly": False,
    "host": "",
    "username": "",
    "#password": "",
    "sshPort": 22,
    "base64StateJson": "",
    "runIncrementalSync": True,
    "sslCa": "",
    "outputBucket": "",
    "base64TableMappingsJson": "",
    "ssl": False,
    "sshPublicKey": "",
    "sshTunnel": False,
    "#sshBase64PrivateKey": "",
    "sshHost": "",
    "sshUser": "",
    "show_binary_log_config": {
        "method": "direct",
        "endpoint_url": "https://app.shipmonk.com/api/keboola/get-binary-logs"
    },
    "inputMappingsJson": "",
    "databases": [],
    "appendMode": False,
    "storageMappingsFile": "mappings",
    "verifyCert": True,
    "debug": True
}

INPUT_MAPPING_TABLE = {
    "selected": True,
    "replication-method": "log_based",
    "columns": {
    }
}


def convert_to_legacy(config: Configuration):
    legacy_cfg = legacy_example.copy()
    legacy_cfg[KEY_MYSQL_PORT] = config.db_settings.port
    legacy_cfg[KEY_MYSQL_HOST] = config.db_settings.host
    legacy_cfg[KEY_MYSQL_USER] = config.db_settings.user
    legacy_cfg[KEY_MYSQL_PWD] = config.db_settings.pswd_password
    # ssh
    legacy_cfg[KEY_USE_SSH_TUNNEL] = config.db_settings.use_ssh
    if config.db_settings.use_ssh:
        legacy_cfg[KEY_SSH_HOST] = config.db_settings.ssh_options.host
        legacy_cfg[KEY_SSH_PORT] = config.db_settings.ssh_options.port
        legacy_cfg[KEY_SSH_USERNAME] = config.db_settings.ssh_options.username

        message_bytes = config.db_settings.ssh_options.pswd_private_key.encode('utf-8')
        legacy_cfg[KEY_SSH_PRIVATE_KEY] = base64.b64encode(message_bytes).decode('utf-8')
    # ssl
    legacy_cfg[KEY_USE_SSL] = config.db_settings.use_ssl
    legacy_cfg[KEY_VERIFY_CERT] = config.db_settings.ssl_options.verifyCert

    legacy_cfg[KEY_MAX_EXECUTION_TIME] = config.advanced_options.max_execution_time
    legacy_cfg['show_binary_log_config'] = dataclasses.asdict(config.sync_options.show_binary_log_config)

    legacy_cfg["fetchObjectsOnly"] = False
    legacy_cfg["storageMappingsFile"] = "mappings"
    legacy_cfg["appendMode"] = config.destination.load_type == LoadType.append_only
    legacy_cfg[KEY_OUTPUT_BUCKET] = config.destination.output_bucket
    legacy_cfg["debug"] = config.debug
    # tables
    legacy_cfg[KEY_DATABASES] = config.source_settings.schemas
    input_mappings = dict()
    for table in config.source_settings.tables:
        schema, table = (table.split('.'))
        if schema not in input_mappings:
            input_mappings[schema] = {"tables": list()}
        table_dict = {table: INPUT_MAPPING_TABLE.copy()}
        input_mappings[schema]["tables"].append(table_dict)
    legacy_cfg[KEY_TABLE_MAPPINGS_JSON] = json.dumps(input_mappings)

    if len(config.source_settings.schemas) > 1:
        config.destination.include_schema_name = True
    legacy_cfg[KEY_INCLUDE_SCHEMA_NAME] = config.destination.include_schema_name

    return legacy_cfg
