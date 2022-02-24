# Email Service

This service is written with FastAPI and is meant to be called internally by certain DAGs that need to trigger BUY/SELL emails to the users subscribed to them. The tickers and corresponding emails (one-to-many) will be stored in Redis in the form:

```
{
    "ticker_1": [
        "bandwagonpatriotsfan@nfl.com",
        "ketchup_izz_spicy@foodie.com",
        ...,
        "iliketurtles@yahoo.net",
    ],
    "ticker_2": [
        "buyhighselllow@cryptogenius.org",
        "anna_delvey@doingtime.io",
        ...,
        "hogwarts_legend@muggles.com",
    ],
    ...,
}
```

## Starting up the Server

To start the server, run `./start_app.sh` from a terminal in this directory. Then, head to `http://127.0.0.1:8000/docs` to see all endpoints and even test them out.

## Endpoints

- add_ticker
- list_tickers
- add_user
- send_alert_email