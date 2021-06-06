import os
import requests
import yaml
import glob
from dataclasses import dataclass
from datetime import datetime, timedelta
from client_alpaca import ClientAlpaca
from client_paper_trade import ClientPaperTrade
from client_db import ClientDB


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time = '2016-01-01'
    _time_frame = '1Min'
    _limit = 10000
    _client_pt = ClientPaperTrade()
    _client_db = ClientDB()

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self._dl_bars_destination = f"{self._dl_destination}/bars"

    def get_bars_segment(
            self,
            symbol: str,
            page_token: str = None) -> dict:
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

    def download_bars_segment(
            self,
            symbol: str,
            page_token: str = None) -> str:
        dl_bars_seg_dst = f"{self._dl_bars_destination}/{symbol}/{self._time_frame}"
        os.makedirs(dl_bars_seg_dst, exist_ok=True)
        file_name = 'head' if page_token is None else page_token
        bars_segment = self.get_bars_segment(symbol, page_token)
        with open(f"{dl_bars_seg_dst}/{file_name}.yaml", 'w') as f:
            yaml.dump(bars_segment, f, indent=2)
        self._logger.debug((
            f"bars_segment is downloaded, "
            f"file_name: \"{file_name}\", "
            f"destination: \"{dl_bars_seg_dst}\""
        ))
        return bars_segment['next_page_token']

    def download_bars(self, symbol: str) -> None:
        next_page_token = None
        while True:
            next_page_token = self.download_bars_segment(symbol, next_page_token)
            if next_page_token is None:
                break
        self._logger.debug(f"all bars are downloaded. token: {symbol}")
        self._client_pt.update_dl_progress_of_symbol(
            symbol=symbol,
            is_complete=True
        )

    def load_bars_lines(self, symbol: str, timeframe: str = '1Min'):
        bars_dir_path = f"{self._dl_bars_destination}/{symbol}/{timeframe}"
        bars_paths = glob.glob(f"{bars_dir_path}/*.yaml")
        # download bars data if not exist it.
        if len(bars_paths) == 0:
            self._logger.debug(f"bars data: \"{symbol}\" is not exist, it will be downloaded.")
            self.download_bars(symbol)
        bars_lines = []
        start_time = datetime.now()
        prev_time = start_time
        for bars_path in bars_paths:
            with open(bars_path, 'r') as f:
                d = yaml.safe_load(f)
            for bar in d['bars']:
                # convert format RFC3339 to mysql_datetime
                bar_time = datetime.strptime(bar['t'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                bars_lines.append([bar_time, symbol, bar['o'], bar['h'], bar['l'], bar['c'], bar['v']])
            now_time = datetime.now()
            self._logger.debug(f"loaded: \"{bars_path}\", load time: \"{now_time - prev_time}\"")
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


def main():
    client = ClientMarketData()
    client.store_bars_to_db('SPY')


if __name__ == '__main__':
    main()
