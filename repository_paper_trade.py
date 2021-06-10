import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
from datetime import datetime, timedelta
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

    def create_tables(self) -> None:
        q_create_assets = self._client_db.load_query_by_name(QueryType.CREATE, self._tbl_name_assets)
        q_create_market_data_dl_progress = self._client_db.load_query_by_name(QueryType.CREATE, self._tbl_name_dl_progress)
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

    def count_market_data_dl_progress(self) -> int:
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

    def _get_df_market_data_dl_progress_active(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame
    ) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_dl_progress)
        df = pd.read_sql(query, self._client_db.conn)
        condition = 'category=="{ct}" & time_frame=="{tf}" & status=="active"'.format(
            ct=category.value,
            tf=time_frame.value
        )
        return df.query(condition)

    def get_symbols_market_data_download_todo(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame,
            to_date: str = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
    ) -> pd.Series:
        # key: asset_id, value: symbol
        condition = f'until.isnull() | until < "{to_date}"'
        df = self._get_df_market_data_dl_progress_active(
            category=category,
            time_frame=time_frame
        ).query(condition).set_index('asset_id')
        return df['symbol']

    def update_market_data_dl_progress(
            self,
            category: PriceDataCategory,
            time_frame: TimeFrame,
            asset_id: str,
            message: str,
            time_until: str
    ) -> None:
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


def main():
    rp = RepositoryPaperTrade()
    print(rp.count_market_data_dl_progress())


if __name__ == '__main__':
    main()
