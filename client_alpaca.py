import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ClientAlpaca:
    _base_url: str
    _api_key: str = os.getenv('ALPACA_API_KEY'),
    _secret_key: str = os.getenv('ALPACA_SECRET_KEY'),
    _dl_destination_path = './data'

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
