import requests
import yaml
import os
import datetime
from dataclasses import dataclass
from client_alpaca import ClientAlpaca
from models.data import MarketDataType, PriceDataType, TimeFrame


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
        self._logger.debug(f'Loading assets data.')
        time_start = datetime.datetime.now()
        with open(self._assets_path, 'r') as f:
            assets = yaml.safe_load(f)
        self._logger.debug(f'Loaded assets. time: "{datetime.datetime.now() - time_start}" sec.')
        return assets

    def init_symbol_dl_progress(self) -> None:
        symbol_dl_progress = {}
        for asset in self.load_assets():
            if asset['status'] != 'active':
                continue
            base_status = {
                'dl_until_time': '',
                'message': ''
            }
            symbol = asset['symbol']
            symbol_dl_progress[symbol] = {
                'id': asset['id'],
                MarketDataType.HIST.value: {
                    PriceDataType.BAR.value: {
                        TimeFrame.MIN.value: base_status.copy(),
                        TimeFrame.HOUR.value: base_status.copy(),
                        TimeFrame.DAY.value: base_status.copy()
                    }
                }
            }
        with open(self._symbol_dl_progress_path, 'w') as f:
            yaml.dump(symbol_dl_progress, f, indent=2)
        self._logger.debug(f'Initialized symbol_dl_progress, saved in "{self._symbol_dl_progress_path}"')

    def load_symbol_dl_progress(self) -> dict:
        if not os.path.exists(self._symbol_dl_progress_path):
            self._logger.debug('symbol_dl_progress is not exist, it will be created.')
            self.init_symbol_dl_progress()
        with open(self._symbol_dl_progress_path, 'r') as f:
            symbol_dl_progress = yaml.safe_load(f)
        return symbol_dl_progress

    def get_symbols_progress_todo(
            self,
            market_dt: MarketDataType = MarketDataType.HIST,
            price_dt: PriceDataType = PriceDataType.BAR,
            time_frame: TimeFrame = TimeFrame.MIN
    ) -> list:
        return [
            symbol for symbol, d in self._symbol_dl_progress.items()
            if d[market_dt.value][price_dt.value][time_frame.value]['dl_until_time'] == ''
        ]

    def update_symbol_dl_progress(self) -> None:
        with open(self._symbol_dl_progress_path, 'w') as f:
            yaml.dump(self._symbol_dl_progress, f, indent=2)
        self._logger.debug(f"in class's \"symbol_data_progress\" is saved in {self._symbol_dl_progress_path}")

    def update_dl_progress_of_symbol(
            self,
            symbol: str,
            dl_until_time: str,
            message: str = '',
            market_dt: MarketDataType = MarketDataType.HIST,
            price_dt: PriceDataType = PriceDataType.BAR,
            time_frame: TimeFrame = TimeFrame.MIN
    ) -> None:
        if symbol in self._symbol_dl_progress:
            # save previous progress data
            prev_dl_until_time = (
                self._symbol_dl_progress[symbol][market_dt.value][price_dt.value][time_frame.value]['dl_until_time']
            )
            prev_message = (
                self._symbol_dl_progress[symbol][market_dt.value][price_dt.value][time_frame.value]['message']
            )
            # update status
            (
                self._symbol_dl_progress[symbol][market_dt.value]
                [price_dt.value][time_frame.value]['dl_until_time']
            ) = dl_until_time
            (
                self._symbol_dl_progress[symbol][market_dt.value]
                [price_dt.value][time_frame.value]['message']
            ) = message
            self.update_symbol_dl_progress()
            # report progress data log
            self._logger.debug((
                f"Updated symbol: \"{symbol}\", "
                f"DL_until_time: \"{prev_dl_until_time}\" -> \"{dl_until_time}\", "
                f"Message: \"{prev_message}\" -> \"{message}\""
            ))
        else:
            self._logger.error(f"Symbol \"{symbol}\" is not exist.")

    def is_symbol_exist(self, symbol: str) -> bool:
        if not (symbol in self._symbol_dl_progress.keys()):
            self._logger.debug(f"symbol \"{symbol}\" is not exist in symbol_dl_progress.")
            return False
        return True

    def is_completed_dl_of_symbol(
            self,
            symbol: str,
            market_dt: MarketDataType = MarketDataType.HIST,
            price_dt: PriceDataType = PriceDataType.BAR,
            time_frame: TimeFrame = TimeFrame.MIN
    ) -> bool:
        dl_comp_date = (
            self._symbol_dl_progress[symbol][market_dt.value][price_dt.value][time_frame.value]['dl_until_time']
        )
        if dl_comp_date != '':
            self._logger.debug(f"symbol \"{symbol}\" is already downloaded.")
            return True
        return False


def main():
    client = ClientPaperTrade()
    client.init_symbol_dl_progress()


if __name__ == '__main__':
    main()
