import os
import requests
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from client_alpaca import ClientAlpaca
from client_db import ClientDB
from repository_paper_trade import RepositoryPaperTrade
from data_types import TimeFrame, PriceDataCategory
from exceptions import AlpacaApiRateLimit


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url: str = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time: str = '2016-01-01'
    _end_time: str = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
    _time_frame: TimeFrame = TimeFrame.MIN
    _limit: int = 10000
    _client_db: ClientDB = ClientDB()
    _repository_pt: RepositoryPaperTrade = RepositoryPaperTrade()
    _api_rate_limit = 200

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self.dl_hist_bars_destination = f"{self._dl_destination}/historical/bars"
        self._api_rate_limit_per_min = (self._api_rate_limit // 59)

    def _request_bar_lines(
            self,
            symbol: str,
            dl_start_time: str,
            page_token: str = None
    ) -> tuple:
        """
        [(time, symbol, open, high, low, close, volume), (...)]
        """
        url = f"{self._base_url}/stocks/{symbol}/{PriceDataCategory.value}"
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
        # lines = [(time, symbol, open, high, low, close, volume), (...)]
        bar_lines = []
        for bar in r.json()['bars']:
            bar_lines.append(
                (bar['t'], r.json()['symbol'], bar['o'], bar['h'], bar['l'], bar['c'], bar['v']))
        return bar_lines, r.json()['next_page_token']

    def download_bars(self, symbol: str) -> list:
        # download bars of symbol
        next_page_token = None
        bars_of_symbol = []
        dl_time_start = self._repository_pt.get_latest_dl_date_of_symbol(symbol)
        if dl_time_start is None:
            dl_time_start = self._start_time
            self._logger.debug(f'Download bar data "{symbol} will start. span: "{dl_time_start}" -> "{self._end_time}".')
        time_start = datetime.now()
        while True:
            bar_lines, next_page_token = self._request_bar_lines(
                symbol=symbol,
                dl_start_time=dl_time_start,
                page_token=next_page_token
            )
            bars_of_symbol.extend(bar_lines)
            if next_page_token is None:
                break
        self._logger.debug(f'Request bars set "{symbol}" are completed. time: "{datetime.now() - time_start}"')
        # update download progress status
        self._repository_pt.update_market_data_dl_progress(
            category=PriceDataCategory.BAR,
            time_frame=self._time_frame.value,
            symbol=symbol,
            message=None,
            time_until=self._end_time
        )
        return bars_of_symbol

    def download_bars_all_symbols(self) -> None:
        # TODO: this func should be moved to repository
        symbols_dl_todo = self._repository_pt.get_symbols_market_data_download_todo(
            category=PriceDataCategory.BAR,
            time_frame=self._time_frame,
            time_until=self._end_time
        )
        download_num = len(symbols_dl_todo)
        self._logger.info(f'Num of download bars: {download_num}')
        time_start = datetime.now()
        for i, symbol in enumerate(symbols_dl_todo):
            self.download_bars(symbol)
            self._logger.info(f'Download progress: {i+1} / {download_num}')
        self._logger.info(f'Download all symbol bars is completed. time: "{datetime.now() - time_start}s"')


def main():
    client = ClientMarketData()
    client.download_bars_all_symbols()


if __name__ == '__main__':
    main()
