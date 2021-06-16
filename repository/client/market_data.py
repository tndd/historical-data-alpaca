import os
import requests
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from repository.client import ClientAlpaca, ClientDB
from data_types import TimeFrame, PriceDataCategory
from exception import AlpacaApiRateLimit


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url: str = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time: str = '2016-01-01'
    _end_time: str = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
    _category: PriceDataCategory = PriceDataCategory.BAR
    _time_frame: TimeFrame = TimeFrame.MIN
    _limit: int = 10000
    _client_db: ClientDB = ClientDB()
    _api_rate_limit = 200

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self._api_rate_limit_per_min = (self._api_rate_limit // 59)

    def request_price_data_segment(
            self,
            symbol: str,
            dl_start_time: str,
            page_token: str = None
    ) -> dict:
        """
        [(time, symbol, open, high, low, close, volume), (...)]
        """
        url = f"{self._base_url}/stocks/{symbol}/{self._category.value}"
        query = {
            'start': dl_start_time,
            'end': self._end_time,
            'timeframe': self._time_frame.value,
            'limit': self._limit
        }
        if not (page_token is None):
            query['page_token'] = page_token
        time_start = datetime.now()
        r = requests.get(
            url,
            headers=self.get_auth_headers(),
            params=query
        )
        time_elapsed = datetime.now() - time_start
        self._logger.debug((
            f"Request symbol: \"{symbol}\", "
            f"Time: \"{time_elapsed}\", "
            f"Status code: \"{r.status_code}\", "
            f"Url: \"{url}\", "
            f"Query: {str(query)}"
        ))
        # raise exception if exceed alpaca api rate limit 200 per min.
        if r.status_code == 429:
            raise AlpacaApiRateLimit(f'Alpaca api rate limit has been exceeded.')
        # wait to avoid api limit rate
        time_too_early = self._api_rate_limit_per_min - time_elapsed.total_seconds()
        if time_too_early > 0:
            self._logger.debug(f'Request time is too early, wait "{time_too_early}" sec.')
            time.sleep(time_too_early)
        return r.json()


def main():
    client = ClientMarketData(
        _end_time='2021-06-03'
    )
    print(client.get_dest_dl_ctg_symbol_timeframe('BEST'))


if __name__ == '__main__':
    main()
