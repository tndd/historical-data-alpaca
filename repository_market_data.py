import os
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
        query = self._client_db.load_query_by_name(QueryType.COUNT, 'bars_1min_symbol')
        self._client_db.cur.execute(query, (symbol,))
        return self._client_db.cur.fetchone()[0]

    def _load_bars_min_dataframe(self, symbol: str) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_bars_min)
        return pd.read_sql(query, self._client_db.conn, params=(symbol,))

    def _load_bars_lines_from_files(self, symbol: str) -> list:
        time_start = datetime.now()
        # if download bars is not completed, it will be downloaded automatically.
        price_data_list = self._client_md.load_price_data(symbol)
        price_data_len = len(price_data_list)
        bars_lines = []
        # time record
        prev_time = time_start
        # convert price data to bars_lines
        for i, price_data in enumerate(price_data_list):
            for bar in price_data['bars']:
                # convert format RFC3339 to mysql_datetime
                bar_time = datetime.strptime(bar['t'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                bars_lines.append([bar_time, symbol, bar['o'], bar['h'], bar['l'], bar['c'], bar['v']])
            # report progress
            now_time = datetime.now()
            self._logger.debug((
                f'Symbol: "{symbol}", '
                f'Making bars lines: "{i + 1}/{price_data_len}", '
                f'Time: "{now_time - prev_time}"'
            ))
            prev_time = now_time
        # sort ascending by time
        bars_lines.sort(key=lambda b: b[0])
        self._logger.info((
            f'Complete Loading bars_lines "{symbol}", '
            f'Total time: "{datetime.now() - time_start}", '
            f'Sort time: "{datetime.now() - prev_time}"'
        ))
        return bars_lines

    def _store_bars_to_db(self, symbol: str) -> None:
        query = self._client_db.load_query_by_name(QueryType.INSERT, self._tbl_name_bars_min)
        bars_lines = self._load_bars_lines_from_files(symbol)
        self._client_db.insert_lines(query, bars_lines)
        self._logger.info(f'Bars "{symbol}" is stored to db.')

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        if self._count_symbol_table_bars_1min(symbol) == 0:
            self._logger.info(f'Bars "{symbol}" is not exist in db, it will be stored.')
            self._store_bars_to_db(symbol)
        return self._load_bars_min_dataframe(symbol)


def main():
    client_md = ClientMarketData(
        _end_time='2021-06-03'
    )
    rp = RepositoryMarketData(
        _client_md=client_md
    )
    bars = rp.load_bars_df('VWO')
    print(bars)


if __name__ == '__main__':
    main()
