from dataclasses import dataclass
from enum import Enum


@dataclass
class TimeFrame(Enum):
    MIN = '1Min'
    HOUR = '1Hour'
    DAY = '1Day'
