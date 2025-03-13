"""
This file exposes configurations from config file and environments as Class Objects
"""
from dotenv import load_dotenv

load_dotenv()
import os.path
import sys
from configparser import BasicInterpolation, ConfigParser
from pydantic import Field, BaseSettings

PROJECT_NAME = "Migration App"


class EnvInterpolation(BasicInterpolation):
    """
    Interpolation which expands environment variables in values.
    """

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)

        if not os.path.expandvars(value).startswith("$"):
            return os.path.expandvars(value)
        else:
            return


try:
    config = ConfigParser(interpolation=EnvInterpolation())
    config.read("conf/application.conf")
except Exception as e:
    print(f"Error while loading the config: {e}")
    print("Failed to Load Configuration. Exiting!!!")
    sys.stdout.flush()
    sys.exit()


class _Service(BaseSettings):
    HOST = config.get("SERVICE", "host")
    PORT = int(config.get("SERVICE", "port"))
    BACKEND_DIR: str = Field(default=".")

class _log(BaseSettings):
    LOG_CONFIG_SECTION = "LOG"
    LOG_BASE_PATH = config.get(LOG_CONFIG_SECTION, 'base_path')
    LOG_LEVEL = config.get(LOG_CONFIG_SECTION, 'level')
    FILE_BACKUP_COUNT = config.get(LOG_CONFIG_SECTION, 'file_backup_count')
    FILE_BACKUP_SIZE = config.get(LOG_CONFIG_SECTION, 'max_log_file_size')
    FILE_NAME = LOG_BASE_PATH + config.get(LOG_CONFIG_SECTION, 'file_name')
    LOG_HANDLERS = config.get(LOG_CONFIG_SECTION, 'handlers')

log = _log()

Service = _Service()
