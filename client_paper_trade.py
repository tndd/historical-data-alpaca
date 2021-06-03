import requests
import json
import os
from dotenv import load_dotenv
from dataclasses import dataclass


@dataclass
class ClientPaperTrade:
    api_key: str
    secret_key: str
    base_url: str
    store_path = './data'

    def get_auth_headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }

    def get_assets(self) -> dict:
        url = f"{self.base_url}/assets"
        r = requests.get(url, headers=self.get_auth_headers())
        return r.json()

    def store_assets(self) -> None:
        assets = self.get_assets()
        with open(f"{self.store_path}/bars/assets.json", 'w') as f:
            json.dump(assets, f, indent=2)


if __name__ == '__main__':
    load_dotenv()
    client = ClientPaperTrade(
        api_key=os.getenv('ALPACA_API_KEY'),
        secret_key=os.getenv('ALPACA_SECRET_KEY'),
        base_url=os.getenv('ALPACA_ENDPOINT_PAPER_TRADE')
    )
    client.store_assets()
