import os
import glob
import yaml
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger, config, Logger
from client_market_data import ClientMarketData
from client_db import ClientDB

os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class RepositoryMarketData:
    _logger: Logger = getLogger(__name__)
    _client_md: ClientMarketData = ClientMarketData()
    _client_db: ClientDB = ClientDB()

    def load_bars_lines_from_file(self, symbol: str) -> list:
        bars_dir_path = self._client_md.get_dl_bars_destination(symbol)
        bars_paths = glob.glob(f"{bars_dir_path}/*.yaml")
        # if download bars is not completed, it will be downloaded.
        self._client_md.download_bars(symbol)
        # recount bars data num
        bars_num = len(bars_paths)
        self._logger.debug(f'symbol: "{symbol}", bars data num: "{bars_num}"')
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
        bars_lines = self.load_bars_lines_from_file(symbol)
        self._client_db.insert_lines_to_historical_bars_1min(bars_lines)
        self._logger.debug(f'bars "{symbol}" is stored to db.')

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        if self._client_db.count_symbol_table_historical_bars_1min(symbol) == 0:
            self._logger.debug(f'bars "{symbol}" is not exist in db, it will be stored.')
            self.store_bars_to_db(symbol)
        return self._client_db.load_table_historical_bars_1min_dataframe(symbol)


def main():
    rp = RepositoryMarketData()
    df = rp.load_bars_df('SPY')
    print(df)


if __name__ == '__main__':
    main()
