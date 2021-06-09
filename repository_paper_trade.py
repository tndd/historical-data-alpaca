import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
from client_db import ClientDB
from client_paper_trade import ClientPaperTrade

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class RepositoryPaperTrade:
    _logger: Logger = getLogger(__name__)
    _client_db: ClientDB = ClientDB()
    _client_pt: ClientPaperTrade = ClientPaperTrade()

    def __post_init__(self) -> None:
        self._create_table_assets()

    def _create_table_assets(self) -> None:
        query = '''
            CREATE TABLE IF NOT EXISTS `assets` (
                `id` char(36) NOT NULL,
                `class` varchar(16) NOT NULL,
                `easy_to_borrow` tinyint(1) NOT NULL,
                `exchange` varchar(16) NOT NULL,
                `fractionable` tinyint(1) NOT NULL,
                `marginable` tinyint(1) NOT NULL,
                `name` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                `shortable` tinyint(1) NOT NULL,
                `status` varchar(16) NOT NULL,
                `symbol` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                `tradable` tinyint(1) NOT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        '''
        self._client_db.cur.execute(query)

    def store_assets_to_db(self) -> None:
        assets = self._client_pt.get_assets()
        query = '''
            INSERT INTO alpaca_market_db.assets (
                id,
                class,
                easy_to_borrow,
                exchange,
                fractionable,
                marginable,
                name,
                shortable,
                status,
                symbol,
                tradable
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        '''
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
        query = '''
            SELECT COUNT(*)
            FROM assets;
        '''
        self._client_db.cur.execute(query)
        return self._client_db.cur.fetchone()[0]

    def load_assets_dataframe(self) -> pd.DataFrame:
        query = '''
            SELECT 
                id,
                class,
                easy_to_borrow,
                exchange,
                fractionable,
                marginable,
                name,
                shortable,
                status,
                symbol,
                tradable
            FROM alpaca_market_db.assets;
        '''
        # if not exist assets data in db, download it.
        if self.count_table_assets() == 0:
            self.store_assets_to_db()
        return pd.read_sql(query, self._client_db.conn)


def main():
    rp = RepositoryPaperTrade()
    df = rp.load_assets_dataframe()
    print(df)


if __name__ == '__main__':
    main()
