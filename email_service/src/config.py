from os import getenv
from dotenv import load_dotenv

load_dotenv()

# Log level
LOG_LEVEL = getenv("LOG_LEVEL")

# Loading in email server info
SMTP_SERVER = getenv("SMTP_SERVER")
SSL_PORT = getenv("SSL_PORT")
SENDER_EMAIL = getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = getenv("SENDER_EMAIL_PASSWORD")

# Loading in IN MEMORY info
EMAIL_MANAGER_HOST = getenv("EMAIL_MANAGER_HOST")
EMAIL_MANAGER_PORT = getenv("EMAIL_MANAGER_PORT")
EMAIL_MANAGER_DB = getenv("EMAIL_MANAGER_DB")
EMAIL_MANAGER_PASSWORD = getenv("EMAIL_MANAGER_PASSWORD")

EMAIL_MANAGER_STORE = getenv("EMAIL_MANAGER_STORE")
EMAIL_MANAGER_INFO = {
    "host": EMAIL_MANAGER_HOST,
    "port": EMAIL_MANAGER_PORT,
    "db": EMAIL_MANAGER_DB,
    "password": EMAIL_MANAGER_PASSWORD,
}
