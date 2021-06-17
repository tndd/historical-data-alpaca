import pandas as pd
import itertools
from typing import Optional
from repository import RepositoryMarketData
from logger_alpaca.logger_alpaca import get_logger


logger = get_logger(__file__)


def make_patterns(ptn_num: int):
    # might be not used
    patterns = list(map(lambda x: ''.join(x), itertools.product('edu', repeat=ptn_num)))
    return patterns


def make_initial_pattern(
        df_bars: pd.DataFrame,
        ptn_num: int = 4
) -> str:
    pattern = ''
    price_prev = df_bars.iloc[0]['close']
    for i in range(1, ptn_num + 1):
        price_now = df_bars.iloc[i]['close']
        if price_prev < price_now:
            pattern += 'u'
        elif price_prev > price_now:
            pattern += 'd'
        else:
            pattern += 'e'
        price_prev = price_now
    return pattern


def make_pattern_df(
        df_bars: pd.DataFrame,
        ptn_num: int = 4,
        ptn_range: int = 100000,
        ptn_start_point: Optional[int] = None
) -> pd.DataFrame:
    if ptn_start_point is None:
        ptn_start_point = ptn_range
    elif ptn_start_point < ptn_range:
        logger.warning('Start point is too early. start point was next to the end of ptn_range')
        ptn_start_point = ptn_range
    return None


def main():
    rp = RepositoryMarketData(
        _end_time='2021-06-15'
    )
    df_bars = rp.load_bars_df('GLD')
    print(df_bars[:5])
    print(make_initial_pattern(df_bars))


if __name__ == '__main__':
    main()