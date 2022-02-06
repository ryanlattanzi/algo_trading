from abc import ABC, abstractmethod
from typing import Dict, Generator
import redis
from pydantic import validate_arguments
from logging import Logger

from algo_trading.logger.default_logger import get_child_logger
from algo_trading.logger.controllers import LogConfig
from algo_trading.config.controllers import PubSubController


class AbstractPubSub(ABC):
    @abstractmethod
    def subscribe(self, channel: str) -> None:
        """Subscribe to a pubsub channel.

        Args:
            channel (str): Channel subscription name.
        """
        pass

    @abstractmethod
    def listen(self) -> Generator:
        """Listen to a channel previously subscribed
        to.

        Yields:
            Generator: Messages in the channel.
        """
        pass

    @abstractmethod
    def publish(self, channel: str, msg: Dict) -> None:
        """Publish a message to a pubsub channel.

        Args:
            channel (str): Channel to publish to.
            msg (Dict): Message to publish as a dict.
        """
        pass


class RedisPubSub(AbstractPubSub):
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
    def pubsub(self) -> redis.Connection:
        try:
            return self._pubsub
        except AttributeError:
            self._pubsub = redis.Redis(**self.redis_info)
            return self._pubsub

    def subscribe(self, channel: str) -> None:
        self.pubsub.subscribe(channel)

    def listen(self) -> Generator:
        return self.pubsub.listen()

    def publish(self, channel: str, msg: Dict) -> None:
        self.pubsub.publish(channel, msg)


class PubSubRepository:
    _pubsub_handlers = {
        PubSubController.redis: RedisPubSub,
    }

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        pubsub_info: Dict,
        pubsub_handler: PubSubController,
        log_info: LogConfig,
    ) -> None:
        """A wrapper class to provide a consistent interface to the
        different PubSubRepository types found in the _pubsub_handlers class
        attribute.

        Args:
            pubsub_info (Dict): Info to connect to a KV store.
            pubsub_handler (KeyValueController): Type of KV store to use.
            log_info (LogConfig): Log info to initialize child log.
        """
        self.pubsub_info = pubsub_info
        self.pubsub_handler = pubsub_handler
        self.log_info = log_info

    @property
    def handler(self) -> AbstractPubSub:
        return PubSubRepository._pubsub_handlers[self.pubsub_handler](
            self.pubsub_info, self.log_info
        )
