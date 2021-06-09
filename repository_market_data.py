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

    def __post_init__(self) -> None:
        self._create_table_bars_1min()

    def _create_table_bars_1min(self) -> None:
        query = '''
            CREATE TABLE IF NOT EXISTS `bars_1min` (
                `time` datetime NOT NULL,
                `symbol` char(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                `open` double NOT NULL,
                `high` double NOT NULL,
                `low` double NOT NULL,
                `close` double NOT NULL,
                `volume` int unsigned NOT NULL,
                PRIMARY KEY (`time`,`symbol`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        '''
        self._client_db.cur.execute(query)

    def _count_symbol_table_bars_1min(self, symbol: str) -> int:
        query = f'''
            SELECT COUNT(*)
            FROM bars_1min
            WHERE symbol = '{symbol}';
        '''
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def _load_table_bars_1min_dataframe(self, symbol: str) -> pd.DataFrame:
        query = f'''
            SELECT `time`, symbol, `open`, high, low, `close`, volume
            FROM alpaca_market_db.bars_1min
            WHERE symbol = '{symbol}'
            order by time
        '''
        return pd.read_sql(query, self._client_db.conn)

    def _load_bars_lines_from_files(self, symbol: str) -> list:
        bars_dir_path = self._client_md.get_dl_bars_destination(symbol)
        bars_paths = glob.glob(f"{bars_dir_path}/*.yaml")
        # if download bars is not completed, it will be downloaded automatically.
        self._client_md.download_bars(symbol)
        # recount bars data num
        bars_num = len(bars_paths)
        self._logger.info(f'symbol: "{symbol}", bars data num: "{bars_num}"')
        bars_lines = []
        # time record
        start_time = datetime.now()
        prev_time = start_time
        # load bars files
        for i, bars_path in enumerate(bars_paths):
            with open(bars_path, 'r') as f:
                d = yaml.safe_load(f)
            for bar in d['bars']:
                # convert format RFC3339 to mysql_datetime
                bar_time = datetime.strptime(bar['t'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                bars_lines.append([bar_time, symbol, bar['o'], bar['h'], bar['l'], bar['c'], bar['v']])
            # report progress
            now_time = datetime.now()
            self._logger.info((
                f"progress: \"{i + 1}/{bars_num}\", "
                f"load time: \"{now_time - prev_time}\", "
                f"loaded: \"{bars_path}\""
            ))
            prev_time = now_time
        # sort ascending by time
        bars_lines.sort(key=lambda b: b[0])
        self._logger.info((
            f"{symbol} bars_lines is loaded. "
            f"total time: \"{datetime.now() - start_time}\", "
            f"sort time: \"{datetime.now() - prev_time}\""
        ))
        return bars_lines

    def _store_bars_to_db(self, symbol: str) -> None:
        query = '''
            INSERT INTO alpaca_market_db.bars_1min (`time`, symbol, `open`, high, low, `close`, volume)
            VALUES(%s, %s, %s, %s, %s, %s, %s);
        '''
        bars_lines = self._load_bars_lines_from_files(symbol)
        self._client_db.insert_lines(query, bars_lines)
        self._logger.info(f'bars "{symbol}" is stored to db.')

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        if self._count_symbol_table_bars_1min(symbol) == 0:
            self._logger.info(f'bars "{symbol}" is not exist in db, it will be stored.')
            self._store_bars_to_db(symbol)
        return self._load_table_bars_1min_dataframe(symbol)


def main():
    rp = RepositoryMarketData()
    df = rp.load_bars_df('SPY')
    print(df)


if __name__ == '__main__':
    main()
