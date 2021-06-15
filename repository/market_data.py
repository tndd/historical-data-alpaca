import os
import shutil
import yaml
import pandas as pd
from dataclasses import dataclass
from glob import glob
from datetime import datetime, timedelta
from logging import Logger
from repository.client import ClientMarketData, ClientDB
from repository import RepositoryPaperTrade
from data_types import TimeFrame, PriceDataCategory, QueryType
from logger_alpaca.logger_alpaca import get_logger
from exceptions import FailDownloadPriceData


@dataclass
class RepositoryMarketData:
    _logger: Logger = get_logger(__name__)
    _start_time: str = '2016-01-01'
    _end_time: str = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
    _category: PriceDataCategory = PriceDataCategory.BAR
    _time_frame: TimeFrame = TimeFrame.MIN
    _client_db: ClientDB = ClientDB()
    _tbl_name_bars_min: str = 'bars_1min'

    def __post_init__(self) -> None:
        self._repository_pt = RepositoryPaperTrade(
            _client_db=self._client_db
        )
        self._client_md = ClientMarketData(
            _start_time=self._start_time,
            _end_time=self._end_time,
            _category=self._category,
            _time_frame=self._time_frame,
            _client_db=self._client_db
        )
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

    def _download_price_data(self, symbol: str) -> None:
        # get latest date of symbol for download
        dl_date_start = self._repository_pt.get_date_should_download(
            category=self._category,
            time_frame=self._time_frame,
            symbol=symbol
        )
        # assign start_time to dl_date_start if dl_date_start is not exist.
        if dl_date_start is None:
            dl_date_start = self._start_time
        # if the latest dl date is newer than end_time, the dl is not executed.
        elif self._end_time < dl_date_start:
            self._logger.debug((
                f'Price Data {symbol} is already downloaded. '
                f'Category: {self._category.value}, '
                f'TimeFrame: {self._time_frame.value}, '
                f'Downloaded until: {dl_date_start}, '
                f'Designated dl until: {self._end_time}'
            ))
            return
        self._logger.debug((
            f'Download price data "{symbol} will start. '
            f'Category: {self._category.value}, '
            f'TimeFrame: {self._time_frame.value}, '
            f'Span: "{dl_date_start}" -> "{self._end_time}".'
        ))
        time_start = datetime.now()
        # make dir for download symbol bars data
        dl_bars_seg_dst = self._client_md.get_dest_dl_ctg_symbol_timeframe(symbol)
        os.makedirs(dl_bars_seg_dst, exist_ok=True)
        # download bars of symbol
        next_page_token = None
        try:
            while True:
                bars_seg = self._client_md.request_price_data_segment(
                    symbol=symbol,
                    dl_start_time=dl_date_start,
                    page_token=next_page_token
                )
                # save bars_seg data in file
                title = f'head_{self._end_time}' if next_page_token is None else next_page_token
                with open(f'{dl_bars_seg_dst}/{title}.yaml', 'w') as f:
                    yaml.dump(bars_seg, f, indent=2)
                # reset next token for download
                if bars_seg['next_page_token'] is None:
                    break
                next_page_token = bars_seg['next_page_token']
            self._logger.info(f'Request bars set "{symbol}" are completed. time: "{datetime.now() - time_start}"')
        except (Exception, KeyboardInterrupt):
            # if download fails, delete the half-files
            shutil.rmtree(dl_bars_seg_dst)
            raise FailDownloadPriceData(f'Downloading price data "{symbol}" is failed. removed the half-download files.')
        # update download progress status
        self._repository_pt.update_market_data_dl_progress(
            category=self._category,
            time_frame=self._time_frame,
            symbol=symbol,
            message=None,
            time_until=self._end_time
        )

    def _load_price_data(self, symbol: str) -> list:
        # preload data in files
        self._download_price_data(symbol)
        price_data_paths = glob(f'{self._client_md.get_dest_dl_ctg_symbol_timeframe(symbol)}/*.yaml')
        prices_len = len(price_data_paths)
        time_start = datetime.now()
        prices_data = []
        time_prev = time_start
        for i, path in enumerate(price_data_paths):
            with open(path, 'r') as f:
                d = yaml.safe_load(f)
                prices_data.append(d)
            # report progress
            time_now = datetime.now()
            self._logger.debug((
                f'Loading "{symbol} from files": {i + 1}/{prices_len}, '
                f'Load time: "{time_now - time_prev}"'
            ))
            time_prev = time_now
        self._logger.info(f'Complete Loading "{symbol}". time: {datetime.now() - time_start}s.')
        return prices_data

    def download_price_data_all(self) -> None:
        symbols_dl_todo = self._repository_pt.get_symbols_market_data_download_todo(
            category=self._category,
            time_frame=self._time_frame,
            time_until=self._end_time
        )
        download_num = len(symbols_dl_todo)
        self._logger.info(f'Num of download bars: {download_num}')
        time_start = datetime.now()
        for i, symbol in enumerate(symbols_dl_todo):
            self._download_price_data(symbol)
            self._logger.info(f'Download progress: {i+1} / {download_num}')
        self._logger.info((
            f'Download all price data is completed. '
            f'Category: {self._category.value}, '
            f'Time frame: {self._time_frame.value}, '
            f'Time: "{datetime.now() - time_start}s"'
        ))

    def _load_bars_lines_from_files(self, symbol: str) -> list:
        time_start = datetime.now()
        price_data_list = self._load_price_data(symbol)
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

    def store_bars_to_db(self, symbol: str) -> None:
        query = self._client_db.load_query_by_name(QueryType.INSERT, self._tbl_name_bars_min)
        bars_lines = self._load_bars_lines_from_files(symbol)
        self._client_db.insert_lines(query, bars_lines)
        self._logger.info(f'Bars "{symbol}" is stored to db.')

    def load_bars_df(self, symbol: str) -> pd.DataFrame:
        if self._count_symbol_table_bars_1min(symbol) == 0:
            self._logger.info(f'Bars "{symbol}" is not exist in db, it will be stored.')
            self.store_bars_to_db(symbol)
        return self._load_bars_min_dataframe(symbol)


def main():
    rp = RepositoryMarketData(
        _end_time='2021-06-05'
    )
    bars = rp._download_price_data('V')
    print(bars)


if __name__ == '__main__':
    main()
