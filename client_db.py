import os
import mysql.connector
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

    def create_connection(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            user=self._user,
            password=self._passwd,
            host=self._host
        )

    def init_db(self) -> None:
        self._cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._name};")
        self._cur.execute(f"USE {self._name};")


def main():
    client = ClientDB()
    print(client)


if __name__ == '__main__':
    main()
