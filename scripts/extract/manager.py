import configparser
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv("../.env")

TEMP_PATH = Path(os.getenv("TEMP_PATH"))
COLLECTION_PATH = Path(os.getenv("COLLECTION_PATH"))
CONFIG_PATH = Path(os.getenv("CONFIG_PATH"))


@dataclass
class PathDefaults:
    """
    class:: PathDefaults
    :module: extract
    :description:

    property:: default
    :type: Path
    :default:

    property:: request
    :type: Path
    :default:
    """

    def __init__(self):
        self._save_raw = COLLECTION_PATH
        self._request = "config/request_config.ini"
        self._temp = TEMP_PATH

    @property
    def save_raw(self):
        return self._save_raw

    @property
    def request(self):
        return self._request

    @property
    def temp(self):
        return self._temp

    def parse_market_params(self) -> configparser.ConfigParser:
        """
        :returns: A proxy object containing the default market parameters.
        :rtype: SectionProxy

        Example Usage:

        code block:: python

            >> path_defaults = PathDefaults()
            >> market_params = path_defaults.parse_market_params()
            >> market_params["ITEM_CODE"]["BABIES_AND_KIDS"]
            11021766

        """

        # path to configuration file in project tree
        config_path = CONFIG_PATH

        # load config parser
        market_request_config_parse = configparser.ConfigParser()

        # read config in directory
        market_request_config_parse.read(config_path)

        return market_request_config_parse
