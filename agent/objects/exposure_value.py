from dataclasses import dataclass
from numbers import Number


@dataclass
class ExposureValue:
    value: Number = 0
    unit: str = '[-]'
