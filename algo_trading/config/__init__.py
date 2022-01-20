from os import getenv
from dotenv import load_dotenv

load_dotenv()

# Loading in DB info
DB_HOST = getenv("POSTGRES_HOST")
DB_PORT = getenv("POSTGRES_PORT")
DB_NAME = getenv("POSTGRES_DB")
DB_USER = getenv("POSTGRES_USER")
DB_PASSWORD = getenv("POSTGRES_PASSWORD")

# Loading in IN MEMORY info
KV_HOST = getenv("REDIS_HOST")
KV_PORT = getenv("REDIS_PORT")
KV_DATABASE = getenv("REDIS_DB")
KV_PASSWORD = getenv("REDIS_PASSWORD")

# Loading in OBJECT STORAGE info
OBJ_STORE_ENDPOINT = getenv("MINIO_ENDPOINT")
OBJ_STORE_REGION = getenv("MINIO_REGION")
OBJ_STORE_ACCESS_KEY = getenv("MINIO_ROOT_USER")
OBJ_STORE_SECRET_KEY = getenv("MINIO_ROOT_PASSWORD")

# Building global vars for processing

DB_INFO = {
    "host": DB_HOST,
    "db_name": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "port": DB_PORT,
}
KV_INFO = {
    "host": KV_HOST,
    "port": KV_PORT,
    "db": KV_DATABASE,
    "password": KV_PASSWORD,
}
OBJ_STORE_INFO = {
    "endpoint_url": OBJ_STORE_ENDPOINT,
    "aws_access_key_id": OBJ_STORE_ACCESS_KEY,
    "aws_secret_access_key": OBJ_STORE_SECRET_KEY,
    "region_name": OBJ_STORE_REGION,
}

DATE_FORMAT = "%Y-%m-%d"
