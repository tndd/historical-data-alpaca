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


if __name__ == '__main__':
    l = make_patterns(3)
    print(l)
