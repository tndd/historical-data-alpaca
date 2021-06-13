import os
from dataclasses import dataclass
from dotenv import load_dotenv
from logging import Logger
from logger_alpaca.logger_alpaca import get_logger

load_dotenv()


@dataclass
class ClientAlpaca:
    _api_key: str = os.getenv('ALPACA_API_KEY')
    _secret_key: str = os.getenv('ALPACA_SECRET_KEY')
    _logger: Logger = get_logger(__name__)

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
