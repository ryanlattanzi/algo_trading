import os
import smtplib
import ssl
import json
from typing import List
from datetime import datetime

from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import KeyValueController
from algo_trading.utils.utils import dt_to_str

from config import (
    SMTP_SERVER,
    SSL_PORT,
    SENDER_EMAIL,
    SENDER_EMAIL_PASSWORD,
    EMAIL_MANAGER_INFO,
    EMAIL_MANAGER_STORE,
)

# TODO: Change the child loggers to be fine if not LOG_INFO is supplied
LOG, LOG_INFO = get_main_logger(
    log_name="email_service",
    file_name=os.path.join("logs", f"email_service_{dt_to_str(datetime.today())}.log"),
    log_level=LogLevelController.info,
)

EMAIL_MANAGER = KeyValueRepository(
    EMAIL_MANAGER_INFO,
    KeyValueController[EMAIL_MANAGER_STORE],
    LOG_INFO,
).handler

# Create a secure SSL context
CONTEXT = ssl.create_default_context()


def get_all_tickers() -> List[str]:
    """
    Gets all keys of k/v store (email manager) which correspond
    to the tickers.

    Returns:
        List[str]: All tickers in the k/v store
    """

    # TODO: add this to the KeyValueRepository
    return EMAIL_MANAGER.conn.keys()


def get_all_users(ticker: str, test: bool = False) -> List[str]:
    """
    Attempt at DRY and dependency inversion so that the method
    of attaining the list of all users is abstracted away
    from the api.

    Args:
        ticker (str): Get all users subscribed to this ticker.
        test (bool, optional): Use to send email to test consumer account. Defaults to False.

    Returns:
        List[str]: List of users' emails for the ticker.
    """

    if test:
        return ["algotrading.consumer@gmail.com"]
    return json.loads(EMAIL_MANAGER.get(ticker))


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


def send_email(message: str, receiver_email: str) -> None:
    """
    Sends the message as an email to the given address.

    Args:
        message (str): Message to send
        receiver_email (str): Receiving email address
    """

    server = create_server()
    try:
        server.sendmail(SENDER_EMAIL, receiver_email, message)
    finally:
        quit_server(server)
