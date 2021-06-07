import os
import requests
import yaml
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from client_alpaca import ClientAlpaca
from client_paper_trade import ClientPaperTrade
from client_db import ClientDB


class SymbolNotDownloadable(Exception):
    pass


class AlpacaApiRateLimit(Exception):
    pass


@dataclass
class TimeFrame(Enum):
    MIN = '1Min'
    HOUR = '1Hour'
    DAY = '1DAY'


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url: str = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time: str = '2016-01-01'
    _time_frame: TimeFrame = TimeFrame.MIN
    _limit: int = 10000
    _client_pt: ClientPaperTrade = ClientPaperTrade()
    _client_db: ClientDB = ClientDB()

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self.dl_hist_bars_destination = f"{self._dl_destination}/historical/bars"

    def _get_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> dict:
        url = f"{self._base_url}/stocks/{symbol}/bars"
        query = {
            'start': self._start_time,
            'end': (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d'),
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
        self._logger.debug((
            f"Request symbol: \"{symbol}\", "
            f"Time: \"{datetime.now() - time_start}\", "
            f"Status code: \"{r.status_code}\", "
            f"Url: \"{url}\", "
            f"Query: {str(query)}"
        ))
        # raise exception if exceed alpaca api rate limit 200 per min.
        if r.status_code == 429:
            raise AlpacaApiRateLimit(f'Alpaca api rate limit has been exceeded.')
        return r.json()

    def get_dl_bars_destination(self, symbol: str) -> str:
        return f"{self.dl_hist_bars_destination}/{symbol}/{self._time_frame.value}"

    def _download_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> str:
        dl_bars_seg_dst = self.get_dl_bars_destination(symbol)
        os.makedirs(dl_bars_seg_dst, exist_ok=True)
        file_name = 'head' if page_token is None else page_token
        bars_segment = self._get_bars_segment(symbol, page_token)
        with open(f"{dl_bars_seg_dst}/{file_name}.yaml", 'w') as f:
            yaml.dump(bars_segment, f, indent=2)
        self._logger.debug((
            f"Download bars_segment is completed. "
            f"File_name: \"{file_name}\", "
            f"Destination: \"{dl_bars_seg_dst}\""
        ))
        return bars_segment['next_page_token']

    def download_bars(self, symbol: str) -> None:
        self._logger.debug(f'Start download bars "{symbol}"')
        if self._client_pt.is_symbol_downloadable(symbol) is False:
            raise SymbolNotDownloadable(f'Symbol "{symbol}" is not downloadable.')
        next_page_token = None
        time_start = datetime.now()
        while True:
            next_page_token = self._download_bars_segment(symbol, next_page_token)
            if next_page_token is None:
                break
        self._logger.debug(f'Download bars data set "{symbol}" are completed. time: "{datetime.now() - time_start}"')
        self._client_pt.update_dl_progress_of_symbol(
            symbol=symbol,
            is_complete=True
        )

    def download_all_symbol_bars(self) -> None:
        dl_symbols = self._client_pt.get_symbols_progress_todo()
        self._logger.debug(f'Todo download symbols bars. num: {len(dl_symbols)}')
        time_start = datetime.now()
        for symbol in dl_symbols:
            self.download_bars(symbol)
        self._logger.debug(f'Download all symbol bars is completed. time: "{datetime.now() - time_start}"')


def main():
    client = ClientMarketData()
    client.download_all_symbol_bars()


if __name__ == '__main__':
    main()
