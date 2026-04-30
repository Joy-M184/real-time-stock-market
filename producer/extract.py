import requests
from config import logger, headers, url
from typing import List, Dict


def connect_to_api() -> List[Dict]:
    """
    Fetches intraday stock market data from the API for a list of stocks.

    Returns:
        List[Dict]: A list of JSON responses for each stock.
    """
    stocks = ['TSLA', 'MSFT', 'GOOGL']
    json_response = []

    for stock in stocks:
        querystring = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": stock,
            "outputsize": "compact",
            "interval": "5min",
            "datatype": "json"
        }
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Stock {stock} loaded successfully")
            json_response.append(data)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching stock {stock}: {e}")
            break

    return json_response


def extract_jason(response: List[Dict]) -> List[Dict[str, str]]:
    """
    Extracts and formats relevant stock data from the API response.

    Args:
        response (List[Dict]): A list of JSON responses from the API.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with extracted stock data.
    """
    log = []

    for stock in response:
        symbol = stock.get("Meta Data", {}).get("2. Symbol")
        time_series = stock.get("Time Series (5min)", {})

        for date_str, metrics in time_series.items():
            entry = {
                "date": date_str,
                "symbol": symbol,
                "open": metrics.get("1. open"),
                "high": metrics.get("2. high"),
                "low": metrics.get("3. low"),
                "close": metrics.get("4. close"),
                "volume": metrics.get("5. volume"),
            }
            log.append(entry)

    return log
