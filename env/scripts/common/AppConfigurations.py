from configparser import BasicInterpolation, ConfigParser
import os.path
from urllib.parse import urlparse


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


CONF_FILE = "conf/settings.conf"
APPLICATION_CONF_FILE = "application_conf/settings.conf"
parser = ConfigParser(interpolation=EnvInterpolation())
config_file = open(CONF_FILE)
parser.read([CONF_FILE, APPLICATION_CONF_FILE])


class DbDetails:
    mongo_uri = parser.get("MONGO", "MONGO_URI")
    parsed_uri = urlparse(mongo_uri)
    database = parser.get("MONGO", "mongo_db")

    arango_db =  parser.get("ARANGO", "arango_db")
    arango_uri =  parser.get("ARANGO", "ARANGO_URI")
    parsed_arango_uri = urlparse(arango_uri)
    arango_user_name =  parser.get("ARANGO", "ARANGO_USERNAME")
    arango_password =  parser.get("ARANGO", "ARANGO_PASSWORD")
