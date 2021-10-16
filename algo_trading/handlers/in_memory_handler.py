from typing import Any, Dict, Union
import redis
import socket
from redis.exceptions import ConnectionError
import json


class RedisHandler:
    def __init__(self, redis_info: Dict) -> None:
        try:
            self.redis_conn = redis.Redis(**redis_info)
        except (socket.gaierror, ConnectionError) as e:
            raise ConnectionError(
                "Could not connect to redis with connection info: " + f"{redis_info}"
            )

    def set(self, key: str, data: Dict[str, Any]) -> None:
        self.redis_conn.set(key, json.dumps(data))

    def get(self, key: str) -> Any:
        return self.redis_conn.get(key)


def get_in_memory_handler(
    handler: str,
    db_info: Dict,
) -> Union[RedisHandler]:
    if handler == "redis":
        return RedisHandler(db_info)


if __name__ == "__main__":
    from os import getenv
    from dotenv import load_dotenv

    load_dotenv("../local.env")

    # Loading in IN MEMORY info
    IN_MEM_HOST = getenv("REDIS_HOST")
    IN_MEM_PORT = getenv("REDIS_PORT")
    IN_MEM_DATABASE = getenv("REDIS_DB")
    IN_MEM_PASSWORD = getenv("REDIS_PASSWORD")

    IN_MEMORY_INFO = {
        "host": IN_MEM_HOST,
        "port": IN_MEM_PORT,
        "db": IN_MEM_DATABASE,
        "password": IN_MEM_PASSWORD,
    }

    r = RedisHandler(IN_MEMORY_INFO)
    r.set("countries", {"Croatia": "Zagreb", "Bahamas": "Nassau"})
    current = json.loads(r.get("countries"))
    print(current["Croatia"])
    current["Croatia"] = "test_val"
    print(current)
    r.set("countries", current)
    print(r.get("countries"))


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
