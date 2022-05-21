class DateNotFoundException(Exception):
    """
    Exception thrown when a requested date does
    not exist in the dataset.

    Args:
        Exception (_type_): _description_
    """

    def __init__(self, date: str, ticker: str) -> None:
        self.date = date
        self.ticker = ticker
        self.message = (
            f"Requested date {self.date} for {self.ticker.upper()} does not exist. "
            + f"Please choose a new date; ensure it is within the ticker's range and falls on a trading day."
        )
        super().__init__(self.message)
