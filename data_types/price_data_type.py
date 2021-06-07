from enum import Enum


class PriceDataType(Enum):
    BAR = 'bars'
    QUOTE = 'quotes'
    TRADE = 'trades'
