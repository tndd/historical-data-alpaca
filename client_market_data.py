import os
import requests
import yaml
from dotenv import load_dotenv
from dataclasses import dataclass
from logging import getLogger, config
from datetime import datetime, timedelta
from client_alpaca import ClientAlpaca

os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')
logger = getLogger(__name__)


@dataclass
class ClientMarketData(ClientAlpaca):
    _start_time = '2016-01-01'
    _time_frame = '1Min'
    _limit = 10000

    def __post_init__(self) -> None:
        self._dl_bars_destination_path = f"{self._dl_destination_path}/bars"

    def get_bars_segment(
            self,
            symbol: str,
            page_token: str = None) -> dict:
        url = f"{self._base_url}/stocks/{symbol}/bars"
        query = {
            'start': self._start_time,
            'end': (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'timeframe': self._time_frame,
            'limit': self._limit
        }
        if not (page_token is None):
            query['page_token'] = page_token
        logger.debug(f"query: {str(query)}")
        r = requests.get(
            url,
            headers=self.get_auth_headers(),
            params=query
        )
        logger.debug(f"request symbol: \"{symbol}\", status code: \"{r.status_code}\"")
        return r.json()

    def download_bars_segment(
            self,
            symbol: str,
            page_token: str = None
    ) -> None:
        os.makedirs(f"{self._dl_bars_destination_path}/{symbol}", exist_ok=True)
        file_name = 'head' if page_token is None else page_token
        bars_segment = self.get_bars_segment(symbol, page_token)
        bars_segment_destination = f"{self._dl_bars_destination_path}/{symbol}/{file_name}.yaml"
        with open(bars_segment_destination, 'w') as f:
            yaml.dump(bars_segment, f, indent=2)
        logger.debug((
            f"bars_segment is downloaded, "
            f"file_name: \"{file_name}\", "
            f"destination: \"{bars_segment_destination}\""
        ))


def main():
    load_dotenv()
    client = ClientMarketData(
        _api_key=os.getenv('ALPACA_API_KEY'),
        _secret_key=os.getenv('ALPACA_SECRET_KEY'),
        _base_url=os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    )
    client.download_bars_segment('SPY')


if __name__ == '__main__':
    main()
