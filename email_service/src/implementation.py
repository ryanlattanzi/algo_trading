import os
import smtplib
import ssl
import json
from typing import List

from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import KeyValueController

from .config import (
    SMTP_SERVER,
    SSL_PORT,
    SENDER_EMAIL,
    SENDER_EMAIL_PASSWORD,
    EMAIL_MANAGER_INFO,
    EMAIL_MANAGER_STORE,
    LOG_LEVEL,
)

# TODO: Change the child loggers to be fine if not LOG_INFO is supplied
LOG, LOG_INFO = get_main_logger(
    log_name="email_service",
    file_name=None,
    log_level=LogLevelController[LOG_LEVEL],
)

EMAIL_MANAGER = KeyValueRepository(
    EMAIL_MANAGER_INFO,
    KeyValueController[EMAIL_MANAGER_STORE],
    LOG_INFO,
).handler

# Create a secure SSL context
CONTEXT = ssl.create_default_context()


def list_tickers() -> List[str]:
    """
    Implementation for API.

    Returns:
        List[str]: All tickers in the k/v store
    """

    # TODO: add this to the KeyValueRepository
    return EMAIL_MANAGER.conn.keys()


def add_ticker(ticker: str) -> None:
    """
    Implementation for API.

    Args:
        ticker (str): New ticker to add.
    """

    EMAIL_MANAGER.set(ticker, [])


def list_users(ticker: str, test: bool) -> List[str]:
    """
    Implementation for API.

    Args:
        ticker (str): Get all users subscribed to this ticker.
        test (bool): Use to send email to test consumer account.

    Returns:
        List[str]: List of users' emails for the ticker.
    """

    if test:
        return ["algotrading.consumer@gmail.com"]

    users = EMAIL_MANAGER.get(ticker)
    if users is None:
        users = "[]"

    return json.loads(users)


def add_users(ticker: str, new_users: List[str]) -> None:
    """
    Implementation for API.

    Args:
        ticker (str): Ticker to add user.
        user (str): User (email) to add.
    """

    current_users = json.loads(EMAIL_MANAGER.get(ticker))
    new_users = current_users + new_users
    EMAIL_MANAGER.set(ticker, new_users)


def create_server() -> smtplib.SMTP_SSL:
    """
    Instead of using a "with" context manager, we want to only
    login one time, so we persist the server object.

    **NOTE**
    It needs to be closed with server.quit() later.

    Returns:
        smtplib.SMTP_SSL: SMTP server to send emails.
    """

    server = smtplib.SMTP_SSL(SMTP_SERVER, SSL_PORT, context=CONTEXT)
    server.login(SENDER_EMAIL, SENDER_EMAIL_PASSWORD)
    return server


def quit_server(server: smtplib.SMTP_SSL) -> None:
    """
    Closes the SMTP email server.

    Args:
        server (smtplib.SMTP_SSL): Server to close
    """

    server.quit()


def send_notification(message: str, receiver_email: str) -> None:
    """
    Implementation for API.

    Args:
        message (str): Message to send
        receiver_email (str): Receiving email address
    """

    server = create_server()
    try:
        server.sendmail(SENDER_EMAIL, receiver_email, message)
    finally:
        quit_server(server)
