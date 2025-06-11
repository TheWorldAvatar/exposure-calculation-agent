from dataclasses import dataclass
from agent.objects.calculation_metadata import CalculationMetadata
from typing import Optional


@dataclass
class CalculationInput:
    subject: str
    exposure: str
    calculation_metadata: CalculationMetadata
