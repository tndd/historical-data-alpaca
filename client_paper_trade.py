import requests
import os
from dataclasses import dataclass
from client_alpaca import ClientAlpaca


@dataclass
class ClientPaperTrade(ClientAlpaca):
    _base_url = os.getenv('ALPACA_ENDPOINT_PAPER_TRADE')

    def __post_init__(self) -> None:
        self._logger = self._logger.getChild(__name__)

    def get_assets(self) -> dict:
        url = f"{self._base_url}/assets"
        r = requests.get(url, headers=self.get_auth_headers())
        self._logger.info(f"Request status code: \"{r.status_code}\"")
        return r.json()


def main():
    client = ClientPaperTrade()
    d = client.get_assets()
    print(d)


if __name__ == '__main__':
    main()
