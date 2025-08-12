from dataclasses import dataclass
from agent.objects.calculation_metadata import CalculationMetadata


@dataclass
class CalculationInput:
    subject: str | list[str]
    exposure: str
    calculation_metadata: CalculationMetadata
