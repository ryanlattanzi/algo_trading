from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Dict, Union, Optional
import redis
import json
from pydantic import validate_arguments
from logging import Logger

from algo_trading.logger.default_logger import get_child_logger
from algo_trading.logger.controllers import LogConfig
from algo_trading.config.controllers import KeyValueController


# things to add to redis
# key: ticker
# value: {
#         last_cross_up: date,
#         last_cross_down: date,
#         last_status: str, (BUY, SELL)
#        }
#
# ALSO - NEED TO THINK ABOUT ORGANIZING DBS IN REDIS:
# (maybe keep this in a postgres table?)
# 0: current tickers we are tracking
# 1: SMA_7_21_50_200 strategy


class AbstractKeyValueRepository(ABC):
    @abstractmethod
    def set(self, key: str, value: Union[str, Dict]) -> bool:
        """Set a key and value. The value can be either string or
        dict. If a dict, we json.dumps the value so it is stored as
        a string and can be json.loads in the application.

        Args:
            key (str): Key to set
            value (Union[Dict[str, Any], str]): Value to set

        Returns:
            bool: Returns True if the operation was successful, False if not.
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Gets the value associated with the given key.

        Args:
            key (str): Key to query.

        Returns:
            Optional[str]: Returns value as a string, or None if the key does
                           not exist in the server.
        """
        pass


class RedisRepository(AbstractKeyValueRepository):
    def __init__(self, redis_info: Dict, log_info: LogConfig) -> None:
        self.redis_info = redis_info
        self.log_info = log_info

    @property
    def log(self) -> Logger:
        try:
            return self._log
        except AttributeError:
            self._log = get_child_logger(
                self.log_info.log_name, self.__class__.__name__
            )
            return self._log

    @property
    def conn(self) -> redis.Connection:
        try:
            return self._conn
        except AttributeError:
            self._conn = redis.Redis(**self.redis_info)
            return self._conn

    def set(self, key: str, value: Union[str, Dict]) -> None:
        if isinstance(value, dict) or isinstance(value, list):
            value = json.dumps(value)
        self.conn.set(key, value)
        self.log.debug(f"Successfully updated {key} to {value}")

    def get(self, key: str) -> Optional[str]:
        return self.conn.get(key)


class FakeKeyValueRepository(AbstractKeyValueRepository):
    def __init__(self, data: Dict, log_info: LogConfig) -> None:
        self.data = data
        self.log_info = log_info

    @property
    def log(self) -> Logger:
        try:
            return self._log
        except AttributeError:
            self._log = get_child_logger(
                self.log_info.log_name, self.__class__.__name__
            )
            return self._log

    def set(self, key: str, value: Union[str, Dict]):
        if type(value) == dict:
            value = json.dumps(value)
        self.data[key] = value
        self.log.debug(f"Successfully updated {key} to {value}")

    def get(self, key: str) -> Optional[str]:
        return self.data[key]


class KeyValueRepository:
    _kv_handlers = {
        KeyValueController.fake: FakeKeyValueRepository,
        KeyValueController.redis: RedisRepository,
    }

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        kv_info: Dict,
        kv_handler: KeyValueController,
        log_info: LogConfig,
    ) -> None:
        """A wrapper class to provide a consistent interface to the
        different KeyValueRepository types found in the _kv_handlers class
        attribute.

        Args:
            kv_info (Dict): Info to connect to a KV store.
            kv_handler (KeyValueController): Type of KV store to use.
        """
        self.kv_info = kv_info
        self.kv_handler = kv_handler
        self.log_info = log_info

    @property
    def handler(self) -> AbstractKeyValueRepository:
        return KeyValueRepository._kv_handlers[self.kv_handler](
            self.kv_info, self.log_info
        )
