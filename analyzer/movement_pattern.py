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


def make_df_price_movement(
        df_bars: pd.DataFrame,
        ptn_num: int = 4,
        ptn_range: int = 100000,
        start_point: Optional[int] = None
) -> pd.DataFrame:
    if start_point is None:
        start_point = ptn_range
    elif start_point < ptn_range:
        logger.warning('Start point is too early. start point was next to the end of ptn_range')
        start_point = ptn_range
    pattern = make_initial_pattern(df_bars[:start_point], ptn_num)
    # make dataframe price movement
    prev_price = df_bars.iloc[ptn_num]['close']
    price_movement_values = []
    for index, row in df_bars[ptn_num + 1:].iterrows():
        bp_close = ((row['close'] - prev_price) / prev_price) * 10000
        bp_high = ((row['high'] - prev_price) / prev_price) * 10000
        bp_low = ((row['low'] - prev_price) / prev_price) * 10000
        price_movement_values.append([row['time'], pattern, bp_close, bp_high, bp_low])
        # update pattern
        if bp_close > 0:
            pattern += 'u'
        elif bp_close < 0:
            pattern += 'd'
        else:
            pattern += 'e'
        # next price
        pattern = pattern[1:]
        prev_price = row['close']

    df_price_movement = pd.DataFrame(
        price_movement_values,
        columns=[
            'time',
            'pattern',
            'bp_close',
            'bp_high',
            'bp_low'
        ]
    )
    return df_price_movement


def make_df_pattern(df_price_movement):
    # categorized movement by pattern
    sr_pattern_up = df_price_movement.groupby('pattern')['bp_close'].apply(
        lambda d: ((d > 0) == True).sum()
    ).to_frame('up')
    sr_pattern_equal = df_price_movement.groupby('pattern')['bp_close'].apply(
        lambda d: ((d == 0) == True).sum()
    ).to_frame('equal')
    sr_pattern_down = df_price_movement.groupby('pattern')['bp_close'].apply(
        lambda d: ((d < 0) == True).sum()
    ).to_frame('down')
    df_price_movement_mean = df_price_movement.groupby('pattern').mean()
    # concat frames for pattern_frame
    return df_price_movement_mean.join([sr_pattern_up, sr_pattern_equal, sr_pattern_down])


def main():
    rp = RepositoryMarketData(
        _end_time='2021-06-15'
    )
    df_bars = rp.load_bars_df('GLD')
    df_price_movement = make_df_price_movement(df_bars)
    df_pattern = make_df_pattern(df_price_movement)
    print(df_bars)
    print(df_price_movement)
    print(df_pattern)
    df_pattern.to_csv('GLD.csv')


if __name__ == '__main__':
    main()