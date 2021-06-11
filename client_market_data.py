import os
import requests
import time
import yaml
from dataclasses import dataclass
from datetime import datetime, timedelta
from client_alpaca import ClientAlpaca
from client_db import ClientDB
from repository_paper_trade import RepositoryPaperTrade
from data_types import TimeFrame, PriceDataCategory
from exceptions import AlpacaApiRateLimit


@dataclass
class ClientMarketData(ClientAlpaca):
    _base_url: str = os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    _start_time: str = '2016-01-01'
    _end_time: str = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
    _category: PriceDataCategory = PriceDataCategory.BAR
    _time_frame: TimeFrame = TimeFrame.MIN
    _limit: int = 10000
    _client_db: ClientDB = ClientDB()
    _repository_pt: RepositoryPaperTrade = RepositoryPaperTrade()
    _api_rate_limit = 200

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)
        self._dest_dl_category = f'{self._dl_destination}/{PriceDataCategory.BAR.value}'
        self._api_rate_limit_per_min = (self._api_rate_limit // 59)

    def _request_price_data_segment(
            self,
            symbol: str,
            dl_start_time: str,
            page_token: str = None
    ) -> dict:
        """
        [(time, symbol, open, high, low, close, volume), (...)]
        """
        url = f"{self._base_url}/stocks/{symbol}/{self._category.value}"
        query = {
            'start': dl_start_time,
            'end': self._end_time,
            'timeframe': self._time_frame.value,
            'limit': self._limit
        }
        if not (page_token is None):
            query['page_token'] = page_token
        time_start = datetime.now()
        r = requests.get(
            url,
            headers=self.get_auth_headers(),
            params=query
        )
        time_elapsed = datetime.now() - time_start
        self._logger.debug((
            f"Request symbol: \"{symbol}\", "
            f"Time: \"{time_elapsed}\", "
            f"Status code: \"{r.status_code}\", "
            f"Url: \"{url}\", "
            f"Query: {str(query)}"
        ))
        # raise exception if exceed alpaca api rate limit 200 per min.
        if r.status_code == 429:
            raise AlpacaApiRateLimit(f'Alpaca api rate limit has been exceeded.')
        # wait to avoid api limit rate
        time_too_early = self._api_rate_limit_per_min - time_elapsed.total_seconds()
        if time_too_early > 0:
            self._logger.debug(f'Request time is too early, wait "{time_too_early}" sec.')
            time.sleep(time_too_early)
        return r.json()

    def _get_dest_dl_ctg_symbol_timeframe(self, symbol: str) -> str:
        return f'{self._dest_dl_category}/{symbol}/{self._time_frame.value}'

    def download_price_data(self, symbol: str) -> None:
        # get latest date of symbol for download
        dl_date_start = self._repository_pt.get_latest_dl_date_of_symbol(
            category=self._category,
            time_frame=self._time_frame,
            symbol=symbol
        )
        # assign start_time to dl_date_start if dl_date_start is not exist.
        if dl_date_start is None:
            dl_date_start = self._start_time
        # if the latest dl date is newer than end_time, the dl is not executed.
        elif self._end_time < dl_date_start:
            self._logger.debug((
                f'Price Data {symbol} is already downloaded. '
                f'Category: {self._category.value}, '
                f'TimeFrame: {self._time_frame.value}, '
                f'Downloaded until: {dl_date_start}, '
                f'Designated dl until: {self._end_time}'
            ))
            return
        self._logger.debug((
            f'Download price data "{symbol} will start. '
            f'Category: {self._category.value}, '
            f'TimeFrame: {self._time_frame.value}, '
            f'Span: "{dl_date_start}" -> "{self._end_time}".'
        ))
        time_start = datetime.now()
        # make dir for download symbol bars data
        dl_bars_seg_dst = self._get_dest_dl_ctg_symbol_timeframe(symbol)
        os.makedirs(dl_bars_seg_dst, exist_ok=True)
        # download bars of symbol
        next_page_token = None
        while True:
            bars_seg = self._request_price_data_segment(
                symbol=symbol,
                dl_start_time=dl_date_start,
                page_token=next_page_token
            )
            # save bars_seg data in file
            title = f'head_{self._end_time}' if next_page_token is None else next_page_token
            with open(f'{dl_bars_seg_dst}/{title}.yaml', 'w') as f:
                yaml.dump(bars_seg, f, indent=2)
            # reset next token for download
            if bars_seg['next_page_token'] is None:
                break
            next_page_token = bars_seg['next_page_token']
        self._logger.debug(f'Request bars set "{symbol}" are completed. time: "{datetime.now() - time_start}"')
        # update download progress status
        self._repository_pt.update_market_data_dl_progress(
            category=self._category,
            time_frame=self._time_frame,
            symbol=symbol,
            message=None,
            time_until=self._end_time
        )

    def download_bars_all_symbols(self) -> None:
        # TODO: this func should be moved to repository
        symbols_dl_todo = self._repository_pt.get_symbols_market_data_download_todo(
            category=self._category,
            time_frame=self._time_frame,
            time_until=self._end_time
        )
        download_num = len(symbols_dl_todo)
        self._logger.info(f'Num of download bars: {download_num}')
        time_start = datetime.now()
        for i, symbol in enumerate(symbols_dl_todo):
            self.download_price_data(symbol)
            self._logger.info(f'Download progress: {i+1} / {download_num}')
        self._logger.info(f'Download all symbol bars is completed. time: "{datetime.now() - time_start}s"')


def main():
    client = ClientMarketData(
        _end_time='2021-06-03'
    )
    client.download_price_data('BEST')


if __name__ == '__main__':
    main()
