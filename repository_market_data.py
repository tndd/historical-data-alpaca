import os
import glob
import yaml
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger, config, Logger
from client_market_data import ClientMarketData
from client_db import ClientDB
from data_types import QueryType

os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class RepositoryMarketData:
    _logger: Logger = getLogger(__name__)
    _client_md: ClientMarketData = ClientMarketData()
    _client_db: ClientDB = ClientDB()
    _tbl_name_bars_min: str = 'bars_1min'

    def __post_init__(self) -> None:
        self._create_tables()

    def _create_tables(self) -> None:
        q_bars_min = self._client_db.load_query_by_name(QueryType.CREATE, self._tbl_name_bars_min)
        self._client_db.cur.execute(q_bars_min)
        self._client_db.conn.commit()

    def _count_symbol_table_bars_1min(self, symbol: str) -> int:
        # TODO: delete maybe
        query = f'''
            SELECT COUNT(*)
            FROM bars_1min
            WHERE symbol = '{symbol}';
        '''
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def _load_bars_min_dataframe(self, symbol: str) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_bars_min)
        return pd.read_sql(query, self._client_db.conn, params=(symbol,))

    def _load_bars_lines_from_files(self, symbol: str) -> list:
        # TODO: delete maybe. because tmp file for saving is not necessary.
        bars_dir_path = self._client_md.get_dest_dl_ctg_symbol_timeframe(symbol)
        bars_paths = glob.glob(f"{bars_dir_path}/*.yaml")
        # if download bars is not completed, it will be downloaded automatically.
        self._client_md._download_price_data(symbol)
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
        # TODO: bars lines will get from market_data_client directly.
        bars_lines = self._load_bars_lines_from_files(symbol)
        self._client_db.insert_lines(query, bars_lines)
        self._logger.info(f'bars "{symbol}" is stored to db.')

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        # TODO: delete maybe. because this func can be integrated into "_load_bars_min_dataframe".
        if self._count_symbol_table_bars_1min(symbol) == 0:
            self._logger.info(f'bars "{symbol}" is not exist in db, it will be stored.')
            self._store_bars_to_db(symbol)
        return self._load_bars_min_dataframe(symbol)


def main():
    rp = RepositoryMarketData()
    df = rp._load_bars_min_dataframe('SPY')
    print(df)


if __name__ == '__main__':
    main()
