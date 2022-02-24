from fastapi import FastAPI
from typing import Dict
from datetime import datetime

from algo_trading.utils.utils import dt_to_str

from utils import send_email, get_all_users, get_all_tickers
from template import EMAIL_ALERT_MESSAGE
from controllers import Signal

app = FastAPI()


@app.post("/tickers")
def add_ticker(ticker: str) -> Dict:
    """
    Adds a new ticker with an empty email list.

    Args:
        ticker (str): Ticker symbol

    Returns:
        Dict: Status
    """


@app.get("/tickers")
def list_tickers() -> Dict:
    """
    Gets a list of all tickers.

    Returns:
        Dict: Status and list of tickers
    """

    # TODO: implement pagination if we have too many keys https://stackoverflow.com/questions/22255589/get-all-keys-in-redis-database-with-python
    return get_all_tickers()


@app.get("/tickers/{ticker}")
def list_users(ticker: str) -> Dict:
    """
    List all users (email) subscribed to a given ticker.

    Args:
        ticker (str):

    Returns:
        Dict: Status and list of users
    """

    return get_all_users(ticker)


@app.put("/tickers/{ticker}")
def add_user(ticker: str, new_user: str) -> Dict:
    """
    Adds a new user to the email list of the
    given ticker.

    Args:
        ticker (str): Ticker to add email
        new_user (str): New user email address

    Returns:
        Dict: Status
    """


@app.post("/tickers/{ticker}")
def send_alert_email(
    ticker: str,
    signal: Signal,
    strategy: str = "SMACross",
    date: str = dt_to_str(datetime.today()),
) -> Dict:
    """
    Sends an email from a template formatted with
    the ticker and signal to all subscribed to that
    ticker.

    Args:
        ticker (str):
        signal (Signal):
        strategy (str): Strategy that triggered the alert.
        date (str): Date for which the alert is relevant.

    Returns:
        Dict: Status
    """

    # Grab email template and format it with the signal
    message = EMAIL_ALERT_MESSAGE.format(
        ticker=ticker,
        signal=signal,
        strategy=strategy,
        date=date,
    )

    # Get all emails associated with ticker from Redis
    email_list = get_all_users(ticker)

    for email in email_list:
        send_email(message, email)
