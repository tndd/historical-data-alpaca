import os
from dotenv import load_dotenv
from dataclasses import dataclass
from logging import getLogger, config
from client_alpaca import ClientAlpaca

os.makedirs('log', exist_ok=True)
config.fileConfig('logging.conf')
logger = getLogger(__name__)


@dataclass
class ClientMarketData(ClientAlpaca):
    pass


def main():
    load_dotenv()
    client = ClientMarketData(
        _api_key=os.getenv('ALPACA_API_KEY'),
        _secret_key=os.getenv('ALPACA_SECRET_KEY'),
        _base_url=os.getenv('ALPACA_ENDPOINT_MARKET_DATA')
    )
    print(client)


if __name__ == '__main__':
    main()
