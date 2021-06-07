import os
import requests
import yaml
import glob
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from client_alpaca import ClientAlpaca
from client_paper_trade import ClientPaperTrade
from client_db import ClientDB


class SymbolNotDownloadable(Exception):
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
        self.dl_bars_destination = f"{self._dl_destination}/bars"

    def _get_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> dict:
        url = f"{self._base_url}/stocks/{symbol}/bars"
        query = {
            'start': self._start_time,
            'end': (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'timeframe': self._time_frame.value,
            'limit': self._limit
        }
        if not (page_token is None):
            query['page_token'] = page_token
        r = requests.get(
            url,
            headers=self.get_auth_headers(),
            params=query
        )
        self._logger.debug((
            f"request symbol: \"{symbol}\", "
            f"status code: \"{r.status_code}\", "
            f"url: \"{url}\", "
            f"query: {str(query)}"
        ))
        return r.json()

    def get_dl_bars_destination(self, symbol: str) -> str:
        return f"{self.dl_bars_destination}/{symbol}/{self._time_frame.value}"

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
            f"bars_segment is downloaded, "
            f"file_name: \"{file_name}\", "
            f"destination: \"{dl_bars_seg_dst}\""
        ))
        return bars_segment['next_page_token']

    def download_bars(self, symbol: str) -> None:
        if self._client_pt.is_symbol_downloadable(symbol) is False:
            raise SymbolNotDownloadable(f'symbol "{symbol}" is not downloadable.')
        next_page_token = None
        while True:
            next_page_token = self._download_bars_segment(symbol, next_page_token)
            if next_page_token is None:
                break
        self._logger.debug(f'all bars data "{symbol}" are downloaded.')
        self._client_pt.update_dl_progress_of_symbol(
            symbol=symbol,
            is_complete=True
        )


def main():
    client = ClientMarketData()
    print(client)


if __name__ == '__main__':
    main()
