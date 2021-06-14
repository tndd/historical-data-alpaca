from repository import RepositoryMarketData


symbols = [
    'AAPL',
    'NVDA',
    'JNJ',
    'PFE',
    'PG',
    'WMT',
    'VZ',
    'T',
    'NEE',
    'DUK'
    'JPM',
    'V',
    'XOM',
    'CVX',
    'NKE',
    'MCD',
    'LMT',
    'UNP',
    'LIN',
    'SHW',
    'AMT',
    'CCI'
]

repo = RepositoryMarketData(
    _end_time='2021-06-05'
)

for symbol in symbols:
    repo.store_bars_to_db(symbol)
