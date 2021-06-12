import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
from typing import Optional
from client_db import ClientDB
from client_paper_trade import ClientPaperTrade
from data_types import QueryType, PriceDataCategory, TimeFrame

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class RepositoryPaperTrade:
    _logger: Logger = getLogger(__name__)
    _client_db: ClientDB = ClientDB()
    _client_pt: ClientPaperTrade = ClientPaperTrade()
    _tbl_name_assets: str = 'assets'
    _tbl_name_dl_progress: str = 'market_data_dl_progress'

    def __post_init__(self) -> None:
        self.create_tables()
        self._init_market_data_dl_progress()

    def create_tables(self) -> None:
        q_create_assets = self._client_db.load_query_by_name(QueryType.CREATE, self._tbl_name_assets)
        q_create_market_data_dl_progress = self._client_db.load_query_by_name(
            QueryType.CREATE,
            self._tbl_name_dl_progress
        )
        self._client_db.cur.execute(q_create_assets)
        self._client_db.cur.execute(q_create_market_data_dl_progress)
        self._client_db.conn.commit()

    def _store_assets_to_db(self) -> None:
        assets = self._client_pt.get_assets()
        query = self._client_db.load_query_by_name(QueryType.INSERT, self._tbl_name_assets)
        lines = [
            (
                a['id'],
                a['class'],
                a['easy_to_borrow'],
                a['exchange'],
                a['fractionable'],
                a['marginable'],
                a['name'],
                a['shortable'],
                a['status'],
                a['symbol'],
                a['tradable']
            )
            for a in assets
        ]
        self._client_db.insert_lines(query, lines)

    def _count_table_assets(self) -> int:
        query = self._client_db.load_query_by_name(QueryType.COUNT, self._tbl_name_assets)
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def _count_market_data_dl_progress(self) -> int:
        query = self._client_db.load_query_by_name(QueryType.COUNT, self._tbl_name_dl_progress)
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def _load_assets_dataframe(self) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_assets)
        # if not exist assets data in db, download it.
        if self._count_table_assets() == 0:
            self._store_assets_to_db()
        return pd.read_sql(query, self._client_db.conn)

    def _init_market_data_dl_progress(self) -> None:
        if self._count_market_data_dl_progress() != 0:
            self._logger.debug(f'Table "{self._tbl_name_dl_progress}" is already initialized.')
            return
        asset_ids = self._load_assets_dataframe()['id']
        lines = []
        categories = [
            PriceDataCategory.BAR.value,
            PriceDataCategory.QUOTE.value,
            PriceDataCategory.TRADE.value
        ]
        time_frames = [
            TimeFrame.MIN.value,
            TimeFrame.HOUR.value,
            TimeFrame.DAY.value
        ]
        default_until = None
        default_message = None
        for asset_id in asset_ids:
            for category in categories:
                for time_frame in time_frames:
                    lines.append((category, time_frame, default_until, default_message, asset_id))
        query = self._client_db.load_query_by_name(QueryType.INSERT, self._tbl_name_dl_progress)
        self._client_db.insert_lines(query, lines)
        self._logger.debug(f'Initialize table "{self._tbl_name_dl_progress}" is completed.')

    def _get_df_market_data_dl_progress_active(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame
    ) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_dl_progress)
        return pd.read_sql(
            query,
            self._client_db.conn,
            params=(category.value, time_frame.value)
        )

    def get_symbols_market_data_download_todo(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame,
            time_until: str
    ) -> list:
        # key: asset_id, value: symbol
        condition = f'until.isnull() | until < "{time_until}"'
        df = self._get_df_market_data_dl_progress_active(
            category=category,
            time_frame=time_frame
        ).query(condition).set_index('asset_id')
        return df['symbol'].values

    def update_market_data_dl_progress(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame,
            symbol: str,
            message: Optional[str],
            time_until: str
    ) -> None:
        asset_id = self._get_df_market_data_dl_progress_active(
            category,
            time_frame
        ).query(f'symbol == "{symbol}"').iat[0, 0]
        query = self._client_db.load_query_by_name(QueryType.UPDATE, self._tbl_name_dl_progress)
        param = (time_until, message, asset_id, category.value, time_frame.value)
        self._client_db.cur.execute(query, param)
        self._client_db.conn.commit()
        self._logger.debug((
            f'Updated dl_progress. '
            f'Category: {category.value}, '
            f'Time frame: {time_frame.value}, '
            f'Asset id: {asset_id}, '
            f'Time until: {time_until}, '
            f'Message: {message}'
        ))

    def get_date_should_download(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame,
            symbol: str
    ) -> Optional[str]:
        """
        explain:
        The start and end dates of the specified download period for the alpaca api are included.
        So when you start a new download, you need to specify the day after the download date.
        """
        df = self._get_df_market_data_dl_progress_active(category, time_frame).set_index('symbol')
        date = df.at[symbol, 'until']
        if date is pd.NaT:
            return None
        return (date + pd.tseries.offsets.Day()).strftime('%Y-%m-%d')


def main():
    rp = RepositoryPaperTrade()
    # sr = rp._get_df_market_data_dl_progress_active(
    #     category=PriceDataCategory.BAR,
    #     time_frame=TimeFrame.MIN,
    # ).set_index('asset_id')['symbol']
    a = rp.get_date_should_download(
        category=PriceDataCategory.BAR,
        time_frame=TimeFrame.MIN,
        symbol='VWO'
    )
    print(a)


if __name__ == '__main__':
    main()
