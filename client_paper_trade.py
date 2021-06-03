import requests
import yaml
import os
import datetime
from dotenv import load_dotenv
from dataclasses import dataclass


@dataclass
class ClientPaperTrade:
    api_key: str
    secret_key: str
    base_url: str
    store_path = './data'
    assets_file_name = 'assets.yaml'
    symbol_dl_progress_file_name = 'symbol_dl_progress.yaml'

    def __post_init__(self) -> None:
        self.assets_path = f"{self.store_path}/{self.assets_file_name}"
        self.symbol_dl_progress_path = f"{self.store_path}/{self.symbol_dl_progress_file_name}"

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

    def get_active_symbols_from_assets(self) -> list:
        assets = self.load_assets()
        active_assets = list(filter(lambda a: a['status'] == 'active', assets))
        return list(map(lambda a: a['symbol'], active_assets))

    def create_symbol_dl_progress(self) -> None:
        symbols = self.get_active_symbols_from_assets()
        time_now = datetime.datetime.now()
        symbol_dl_progress = dict()
        for s in symbols:
            detail = {
                'f': False,
                't': time_now.isoformat(),
                'm': ''
            }
            symbol_dl_progress[s] = detail
        with open(self.symbol_dl_progress_path, 'w') as f:
            yaml.dump(symbol_dl_progress, f, indent=2)

    def load_symbol_dl_progress(self) -> dict:
        if not os.path.exists(self.symbol_dl_progress_path):
            self.create_symbol_dl_progress()
        with open(self.symbol_dl_progress_path, 'r') as f:
            symbol_dl_progress = yaml.safe_load(f)
        return symbol_dl_progress

    def load_symbols_progress_todo(self) -> list:
        sdp = self.load_symbol_dl_progress()
        return [s for s, d in sdp.items() if d['f'] is False]


def main():
    load_dotenv()
    client = ClientPaperTrade(
        api_key=os.getenv('ALPACA_API_KEY'),
        secret_key=os.getenv('ALPACA_SECRET_KEY'),
        base_url=os.getenv('ALPACA_ENDPOINT_PAPER_TRADE')
    )
    symbols = client.load_symbols_progress_todo()
    print(symbols)


if __name__ == '__main__':
    main()
