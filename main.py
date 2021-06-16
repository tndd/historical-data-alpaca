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
    'SHW',
    'AMT',
]

repo = RepositoryMarketData(
    _end_time='2021-06-05'
)

for symbol in symbols:
    repo.update_bars_in_db(symbol)
