from repository import RepositoryMarketData
from logger_alpaca.logger_alpaca import get_logger
from create_dataframe import make_df_price_movement, make_df_pattern


logger = get_logger(__file__)


def main():
    rp = RepositoryMarketData(
        _end_time='2021-06-15'
    )
    df_bars = rp.load_bars_df('BEST')
    df_price_movement = make_df_price_movement(df_bars)
    df_pattern = make_df_pattern(df_price_movement)
    print(df_bars)
    print(df_price_movement)
    print(df_pattern)


if __name__ == '__main__':
    main()
