from repository import RepositoryMarketData


symbols = [
    'AAPL',
    'JNJ',
    'WMT',
    'VZ',
    'NEE',
    'JPM',
    'XOM',
    'NKE',
    'LMT',
    'LIN',
    'AMT',
]

repo = RepositoryMarketData(
    _end_time='2021-06-05'
)

for symbol in symbols:
    repo.store_bars_to_db(symbol)
