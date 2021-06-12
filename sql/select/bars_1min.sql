SELECT `time`, symbol, `open`, high, low, `close`, volume
FROM alpaca_market_db.bars_1min
WHERE symbol = %s
order by time
