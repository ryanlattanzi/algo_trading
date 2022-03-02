from os import getenv
from dotenv import load_dotenv

load_dotenv()

# Loading in DB info
DB_HOST = getenv("POSTGRES_HOST")
DB_PORT = getenv("POSTGRES_PORT")
DB_NAME = getenv("POSTGRES_DB")
DB_USER = getenv("POSTGRES_USER")
DB_PASSWORD = getenv("POSTGRES_PASSWORD")

# Building global vars for processing

DB_INFO = {
    "host": DB_HOST,
    "db_name": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "port": DB_PORT,
}
