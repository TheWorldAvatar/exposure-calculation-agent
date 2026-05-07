from dataclasses import dataclass
from agent.objects.calculation_metadata import CalculationMetadata
from agent.objects.exposure_dataset import ExposureDataset


@dataclass
class ExposureResultSet:
    calculation_metadata: CalculationMetadata
    exposure_dataset: ExposureDataset

    def __eq__(self, other):
        return isinstance(other, ExposureResultSet) and self.calculation == other.calculation and self.exposure == other.exposure

    def __hash__(self):
        return hash((self.calculation_metadata.iri, self.exposure_dataset.iri))
