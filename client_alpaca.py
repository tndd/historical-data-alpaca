import os
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import getLogger, config, Logger

load_dotenv()
os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')


@dataclass
class ClientAlpaca:
    _api_key: str = os.getenv('ALPACA_API_KEY')
    _secret_key: str = os.getenv('ALPACA_SECRET_KEY')
    _dl_destination = './api_data'
    _logger: Logger = getLogger(__name__)

    def get_auth_headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._secret_key
        }


def main():
    client = ClientAlpaca()
    print(client)


if __name__ == '__main__':
    main()
