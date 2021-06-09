import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
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

    def store_assets_to_db(self) -> None:
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

    def count_table_assets(self) -> int:
        query = self._client_db.load_query_by_name(QueryType.COUNT, self._tbl_name_assets)
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def load_assets_dataframe(self) -> pd.DataFrame:
        query = self._client_db.load_query_by_name(QueryType.SELECT, self._tbl_name_assets)
        # if not exist assets data in db, download it.
        if self.count_table_assets() == 0:
            self.store_assets_to_db()
        return pd.read_sql(query, self._client_db.conn)

    def init_market_data_dl_progress(self):
        # extract only active rows
        asset_ids = self.load_assets_dataframe()['id']
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

    def get_df_market_data_dl_progress_active(
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

    def get_df_market_data_dl_progress_active_bars_min(self) -> pd.DataFrame:
        category = PriceDataCategory.BAR
        time_frame = TimeFrame.MIN
        return self.get_df_market_data_dl_progress_active(category, time_frame)


def main():
    rp = RepositoryPaperTrade()
    df = rp.get_df_market_data_dl_progress_active_bars_min()
    print(df)


if __name__ == '__main__':
    main()
