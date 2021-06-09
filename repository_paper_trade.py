import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger
from client_db import ClientDB

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class RepositoryPaperTrade:
    _logger: Logger = getLogger(__name__)
    _client_db: ClientDB = ClientDB()

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
                `name` varchar(128) NOT NULL,
                `shortable` tinyint(1) NOT NULL,
                `status` varchar(16) NOT NULL,
                `symbol` varchar(8) NOT NULL,
                `tradable` tinyint(1) NOT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        '''
        self._client_db.cur.execute(query)


def main():
    rp = RepositoryPaperTrade()


if __name__ == '__main__':
    main()
