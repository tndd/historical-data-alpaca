INSERT INTO alpaca_market_db.assets (
    id,
    class,
    easy_to_borrow,
    exchange,
    fractionable,
    marginable,
    name,
    shortable,
    status,
    symbol,
    tradable
) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
