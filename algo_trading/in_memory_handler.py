from typing import Any, Dict, Union
import redis
import socket
from redis.exceptions import ConnectionError


class RedisHandler:
    def __init__(self, redis_info: Dict) -> None:
        print(redis_info)
        try:
            self.redis_conn = redis.Redis(**redis_info)
        except (socket.gaierror, ConnectionError) as e:
            print("bruh")

    def set(self, data: Dict[str, Any]) -> None:
        self.redis_conn.mset(data)

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
    r.set({"Croatia": "Zagreb", "Bahamas": "Nassau"})
    print(r.get("Bahamas"))


# things to add to redis
# key: ticker
# value: {
#         last_cross_up: date,
#         bear_cross_up: bool,
#         last_cross_down: date,
#        }
