import os
import mysql.connector
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class ClientDB:
    _user: str = os.getenv('DB_USER')
    _passwd: str = os.getenv('DB_PASSWORD')
    _host: str = os.getenv('DB_HOST')
    _name: str = os.getenv('DB_NAME')
    _logger: Logger = getLogger(__name__)

    def __post_init__(self) -> None:
        self._conn = self.create_connection()
        self._cur = self._conn.cursor()
        self.init_db()
        self.create_table_historical_bars_1min()

    def create_connection(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            user=self._user,
            password=self._passwd,
            host=self._host
        )

    def init_db(self) -> None:
        self._cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._name};")
        self._cur.execute(f"USE {self._name};")

    def create_table_historical_bars_1min(self) -> None:
        query = '''
                CREATE TABLE IF NOT EXISTS `historical_bars_1min` (
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
        self._cur.execute(query)

    def insert_lines_to_historical_bars_1min(self, lines: list) -> None:
        # split lines every 500,000
        chunk = 500000
        lines_len = len(lines)
        self._logger.debug(f"insert num: {lines_len}")
        lines_parts = [lines[i:i+chunk] for i in range(0, lines_len, chunk)]
        query = '''
                INSERT INTO alpaca_market_db.historical_bars_1min (
                    `time`, symbol, `open`, high, low, `close`, volume
                ) VALUES(%s, %s, %s, %s, %s, %s, %s);
                '''
        for i, l_part in enumerate(lines_parts):
            self._cur.executemany(query, l_part)
            self._logger.debug(f"executed query. progress: {i + 1}/{(lines_len // chunk) + 1}")
        self._conn.commit()

    def count_symbol_table_historical_bars_1min(self, symbol: str) -> int:
        query = f'''
            SELECT COUNT(*) FROM historical_bars_1min
            WHERE symbol = '{symbol}';
        '''
        self._cur.execute(query)
        return self._cur.fetchone()[0]

    def load_table_historical_bars_1min_dataframe(self, symbol: str) -> pd.DataFrame:
        query = f'''
            SELECT `time`, symbol, `open`, high, low, `close`, volume
            FROM alpaca_market_db.historical_bars_1min
            WHERE symbol = '{symbol}'
            order by time
        '''
        return pd.read_sql(query, self._conn)


def main():
    client = ClientDB()
    n = client.count_symbol_table_historical_bars_1min('SPY')
    print(n)


if __name__ == '__main__':
    main()
