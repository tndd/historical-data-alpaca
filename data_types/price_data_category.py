from enum import Enum


class PriceDataCategory(Enum):
    BAR = 'bars'
    QUOTE = 'quotes'
    TRADE = 'trades'
