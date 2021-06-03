import requests
import yaml
import os
from dotenv import load_dotenv
from dataclasses import dataclass


@dataclass
class ClientPaperTrade:
    api_key: str
    secret_key: str
    base_url: str
    store_path = './data'
    assets_file_name = 'assets.yaml'

    def __post_init__(self) -> None:
        self.assets_path = f"{self.store_path}/{self.assets_file_name}"

    def get_auth_headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }

    def get_assets(self) -> dict:
        url = f"{self.base_url}/assets"
        r = requests.get(url, headers=self.get_auth_headers())
        return r.json()

    def download_assets(self) -> None:
        assets = self.get_assets()
        with open(self.assets_path, 'w') as f:
            yaml.dump(assets, f, indent=2)

    def load_assets(self) -> dict:
        # if not exist assets data, download it.
        if not os.path.exists(self.assets_path):
            self.download_assets()
        with open(self.assets_path, 'r') as f:
            assets = yaml.safe_load(f)
        return assets

    def get_active_symbols(self) -> list:
        assets = self.load_assets()
        active_assets = list(filter(lambda a: a['status'] == 'active', assets))
        return list(map(lambda a: a['symbol'], active_assets))


def main():
    load_dotenv()
    client = ClientPaperTrade(
        api_key=os.getenv('ALPACA_API_KEY'),
        secret_key=os.getenv('ALPACA_SECRET_KEY'),
        base_url=os.getenv('ALPACA_ENDPOINT_PAPER_TRADE')
    )
    symbols = client.get_active_symbols()
    print(symbols)
    # client.store_assets()


if __name__ == '__main__':
    main()
