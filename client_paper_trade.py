import requests
import yaml
import os
import datetime
from dataclasses import dataclass
from client_alpaca import ClientAlpaca


@dataclass
class ClientPaperTrade(ClientAlpaca):
    _assets_name = 'assets.yaml'
    _symbol_dl_progress_name = 'symbol_dl_progress.yaml'
    _base_url = os.getenv('ALPACA_ENDPOINT_PAPER_TRADE')

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self._assets_path = f"{self._dl_destination}/{self._assets_name}"
        self._symbol_dl_progress_path = f"{self._dl_destination}/{self._symbol_dl_progress_name}"
        self._symbol_dl_progress = self.load_symbol_dl_progress()

    def get_assets(self) -> dict:
        url = f"{self._base_url}/assets"
        r = requests.get(url, headers=self.get_auth_headers())
        self._logger.debug(f"request status code: \"{r.status_code}\"")
        return r.json()

    def download_assets(self) -> None:
        assets = self.get_assets()
        with open(self._assets_path, 'w') as f:
            yaml.dump(assets, f, indent=2)
        self._logger.debug(f"assets is downloaded in \"{self._assets_path}\"")

    def load_assets(self) -> dict:
        # if not exist assets data, download it.
        if not os.path.exists(self._assets_path):
            self._logger.debug('assets data is not exist, it will be downloaded.')
            self.download_assets()
        with open(self._assets_path, 'r') as f:
            assets = yaml.safe_load(f)
        return assets

    def get_active_symbols_from_assets(self) -> list:
        assets = self.load_assets()
        active_assets = list(filter(lambda a: a['status'] == 'active', assets))
        return list(map(lambda a: a['symbol'], active_assets))

    def init_symbol_dl_progress(self) -> None:
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
        with open(self._symbol_dl_progress_path, 'w') as f:
            yaml.dump(symbol_dl_progress, f, indent=2)
        self._logger.debug(f"symbol_dl_progress is initialized, saved in \"{self._symbol_dl_progress_path}\"")

    def load_symbol_dl_progress(self) -> dict:
        if not os.path.exists(self._symbol_dl_progress_path):
            self._logger.debug('symbol_dl_progress is not exist, it will be created.')
            self.init_symbol_dl_progress()
        with open(self._symbol_dl_progress_path, 'r') as f:
            symbol_dl_progress = yaml.safe_load(f)
        return symbol_dl_progress

    def symbols_progress_todo(self) -> list:
        return [s for s, d in self._symbol_dl_progress.items() if d['f'] is False]

    def update_symbol_dl_progress(self) -> None:
        with open(self._symbol_dl_progress_path, 'w') as f:
            yaml.dump(self._symbol_dl_progress, f, indent=2)
        self._logger.debug(f"in class's \"symbol_data_progress\" is saved in {self._symbol_dl_progress_path}")

    def update_dl_progress_of_symbol(
            self,
            symbol: str,
            is_complete: bool,
            message: str = '') -> None:
        if symbol in self._symbol_dl_progress:
            # save previous progress data
            prev_updated_time = self._symbol_dl_progress[symbol]['t']
            prev_is_complete_flag = self._symbol_dl_progress[symbol]['f']
            prev_error_message = self._symbol_dl_progress[symbol]['m']
            # update progress data
            self._symbol_dl_progress[symbol]['t'] = datetime.datetime.now().isoformat()
            self._symbol_dl_progress[symbol]['f'] = is_complete
            self._symbol_dl_progress[symbol]['m'] = message
            self.update_symbol_dl_progress()
            # report progress data log
            self._logger.debug((
                f"update symbol: \"{symbol}\", "
                f"is_complete: \"{prev_is_complete_flag}\" -> \"{self._symbol_dl_progress[symbol]['f']}\", "
                f"error_message: \"{prev_error_message}\" -> \"{self._symbol_dl_progress[symbol]['m']}\", "
                f"update_time: \"{prev_updated_time}\" -> \"{self._symbol_dl_progress[symbol]['t']}\""
            ))
        else:
            self._logger.error(f"symbol \"{symbol}\" is not exist.")

    def is_symbol_downloadable(self, symbol: str) -> bool:
        # is exist symbol?
        if not (symbol in self._symbol_dl_progress.keys()):
            self._logger.debug(f"symbol: \"{symbol}\" is not exist in symbol_dl_progress.")
            return False
        # is completed download?
        elif self._symbol_dl_progress[symbol]['f'] is True:
            self._logger.debug(f"symbol: \"{symbol}\" is already downloaded.")
        else:
            return True


def main():
    client = ClientPaperTrade()
    print(client.get_auth_headers())


if __name__ == '__main__':
    main()
