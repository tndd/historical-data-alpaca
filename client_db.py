import os
import mysql.connector
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
from pathlib import Path
from exceptions import NotExistSqlFile

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class ClientDB:
    _logger: Logger = getLogger(__name__)
    _user: str = os.getenv('DB_USER')
    _passwd: str = os.getenv('DB_PASSWORD')
    _host: str = os.getenv('DB_HOST')
    _name: str = os.getenv('DB_NAME')
    _sql_dir_path: str = './sql'

    def __post_init__(self) -> None:
        self.conn = self.create_connection()
        self.cur = self.conn.cursor()
        self.init_db()

    def get_sql_file_path(self, file_name: str) -> str:
        file_path = f'{Path(__file__).parent}/{self._sql_dir_path}/{file_name}.sql'
        if not os.path.exists(file_path):
            raise NotExistSqlFile(f'Not exist sql file. name: "{file_name}".sql')
        return file_path

    def load_query_by_name(self, file_name: str) -> str:
        with open(self.get_sql_file_path(file_name), 'r') as f:
            q = f.read()
        return q

    def create_connection(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            user=self._user,
            password=self._passwd,
            host=self._host
        )

    def init_db(self) -> None:
        self.cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._name};")
        self.cur.execute(f"USE {self._name};")

    def insert_lines(self, query: str, lines: list) -> None:
        # split lines every 500,000 because of restriction memory limit.
        chunk = 500000
        lines_len = len(lines)
        self._logger.info(f"insert num: {lines_len}")
        lines_parts = [lines[i:i + chunk] for i in range(0, lines_len, chunk)]
        for i, l_part in enumerate(lines_parts):
            self.cur.executemany(query, l_part)
            self._logger.info(f"executed query. progress: {i + 1}/{(lines_len // chunk) + 1}")
        self.conn.commit()


def main():
    client = ClientDB()
    print(client.load_query_by_name('create_table_assets'))


if __name__ == '__main__':
    main()
