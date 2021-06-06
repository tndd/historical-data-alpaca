import os
import requests
import yaml
import glob
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta
from client_alpaca import ClientAlpaca
from client_paper_trade import ClientPaperTrade
from client_db import ClientDB


class SymbolIsNotDownloadable(Exception):
    pass


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time = '2016-01-01'
    _time_frame = '1Min'
    _limit = 10000
    _client_db = ClientDB()

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self._dl_bars_destination = f"{self._dl_destination}/bars"

    def _get_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> dict:
        url = f"{self._base_url}/stocks/{symbol}/bars"
        query = {
            'start': self._start_time,
            'end': (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'timeframe': self._time_frame,
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

    def _download_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> str:
        dl_bars_seg_dst = f"{self._dl_bars_destination}/{symbol}/{self._time_frame}"
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

    def download_bars(
            self,
            symbol: str,
            client_pt: ClientPaperTrade = ClientPaperTrade()
    ) -> None:
        if client_pt.is_symbol_downloadable(symbol) is False:
            raise SymbolIsNotDownloadable(f'symbol "{symbol}" is not downloadable.')
        next_page_token = None
        while True:
            next_page_token = self._download_bars_segment(symbol, next_page_token)
            if next_page_token is None:
                break
        self._logger.debug(f"all bars are downloaded. symbol: {symbol}")
        client_pt.update_dl_progress_of_symbol(
            symbol=symbol,
            is_complete=True
        )

    # the client should only be responsible for the communication part of the api.
    # TODO: Separate under functions to "repository_market_data"
    def load_bars_lines(self, symbol: str) -> list:
        bars_dir_path = f"{self._dl_bars_destination}/{symbol}/{self._time_frame}"
        bars_paths = glob.glob(f"{bars_dir_path}/*.yaml")
        # download bars data if not exist it.
        if len(bars_paths) == 0:
            self._logger.debug(f'bars data files "{symbol}" is not exist, it will be downloaded.')
            self.download_bars(symbol)
        # recount bars data num
        bars_num = len(bars_paths)
        self._logger.debug(f"symbol: \"{symbol}\" bars data num: \"{bars_num}\"")
        bars_lines = []
        start_time = datetime.now()
        prev_time = start_time
        for i, bars_path in enumerate(bars_paths):
            with open(bars_path, 'r') as f:
                d = yaml.safe_load(f)
            for bar in d['bars']:
                # convert format RFC3339 to mysql_datetime
                bar_time = datetime.strptime(bar['t'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                bars_lines.append([bar_time, symbol, bar['o'], bar['h'], bar['l'], bar['c'], bar['v']])
            now_time = datetime.now()
            self._logger.debug((
                f"progress: \"{i + 1}/{bars_num}\", "
                f"load time: \"{now_time - prev_time}\", "
                f"loaded: \"{bars_path}\""
            ))
            prev_time = now_time
        # sort ascending by time
        bars_lines.sort(key=lambda b: b[0])
        self._logger.debug((
            f"{symbol} bars_lines is loaded. "
            f"total time: \"{datetime.now() - start_time}\", "
            f"sort time: \"{datetime.now() - prev_time}\""
        ))
        return bars_lines

    def store_bars_to_db(self, symbol: str) -> None:
        bars_lines = self.load_bars_lines(symbol)
        self._client_db.insert_lines_to_historical_bars_1min(bars_lines)
        self._logger.debug(f"bars \"{symbol}\" is stored to db.")

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        if self._client_db.count_symbol_table_historical_bars_1min(symbol) == 0:
            self._logger.debug(f'bars "{symbol}" is not exist in db, it will be stored.')
            self.store_bars_to_db(symbol)
        return self._client_db.load_table_historical_bars_1min_dataframe(symbol)


def main():
    client = ClientMarketData()
    df = client.load_bars_df('UNCH')
    print(df)


if __name__ == '__main__':
    main()
