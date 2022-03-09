from fastapi import FastAPI
from typing import Dict, List
from datetime import datetime


from src import implementation
from src.template import EMAIL_ALERT_MESSAGE
from src.controllers import NotificationPayload

"""
API Design based off of:
https://stackoverflow.com/questions/12310442/how-to-design-rest-api-for-email-sending-service
"""

app = FastAPI()


@app.get("/tickers")
def list_tickers() -> Dict:
    """
    Gets a list of all tickers.

    Returns:
        Dict: Status and list of tickers
    """

    # TODO: implement pagination if we have too many keys https://stackoverflow.com/questions/22255589/get-all-keys-in-redis-database-with-python
    return {
        "status": 200,
        "body": implementation.list_tickers(),
    }


@app.post("/tickers")
def add_ticker(ticker: str) -> Dict:
    """
    Adds a new ticker with an empty email list.

    Args:
        ticker (str): Ticker symbol

    Returns:
        Dict: Status
    """

    implementation.add_ticker(ticker)

    return {
        "status": 200,
        "body": f"Successfully added ticker {ticker}",
    }


@app.get("/tickers/{ticker}")
def list_users(ticker: str, test: bool = False) -> Dict:
    """
    List all users (email) subscribed to a given ticker.

    Args:
        ticker (str):
        test (bool): Use to return the test consumer account.

    Returns:
        Dict: Status and list of users
    """

    return {
        "status": 200,
        "body": implementation.list_users(ticker, test),
    }


@app.put("/tickers/{ticker}")
def add_users(ticker: str, new_users: List[str]) -> Dict:
    """
    Adds a new user to the email list of the
    given ticker.

    Args:
        ticker (str): Ticker to add email.
        new_user (List[str]): List of email addresses to add.

    Returns:
        Dict: Status
    """

    implementation.add_users(ticker, new_users)

    return {
        "status": 200,
        "body": f"Successfully added users {new_users} to ticker {ticker}",
    }


@app.post("/notification/{ticker}/send")
def send_notification(
    ticker: str,
    payload: NotificationPayload,
) -> Dict:
    """
    Sends a notification from a template formatted with
    the ticker and signal to all subscribed to that
    ticker.

    Args:
        ticker (str):
        payload (NotificationPayload):

    Returns:
        Dict: Status
    """

    message = EMAIL_ALERT_MESSAGE.format(
        ticker=ticker,
        signal=payload.signal,
        strategy=payload.strategy,
        date=payload.date,
    )

    for email in implementation.list_users(ticker, payload.test):
        implementation.send_notification(message, email)

    return {
        "status": 200,
        "body": f"Successfully sent {payload.signal} notification for ticker {ticker}",
    }
