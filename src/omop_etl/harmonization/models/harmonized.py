from dataclasses import field, dataclass
from typing import Iterable, Dict, Any, Callable, List

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.core.serialize import (
    to_normalized,
    build_nested_df,
    to_wide,
    export_leaf_object,
)


@dataclass
class HarmonizedData:
    """
    Stores all patient data for a processed trial
    """

    trial_id: str
    patients: List[Patient] = field(default_factory=list)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.trial_id}, {self.patients}"

    def filter(self, predicate: Callable[[Patient], bool]) -> HarmonizedData:
        """
        Filter patients using a predicate function.

        Args:
            predicate: Function that returns True for patients to include

        Returns:
            New HarmonizedData with filtered patients
        """
        filtered_patients = [p for p in self.patients if predicate(p)]
        return HarmonizedData(
            trial_id=self.trial_id,
            patients=filtered_patients,
        )

    def to_dict(self) -> Dict[str, Any]:
        patients = []
        for p in self.patients:
            # include ids
            d = export_leaf_object(p, exclude=set())
            # trial_id present in each record
            d.setdefault("trial_id", self.trial_id)
            patients.append(d)
        return {"trial_id": self.trial_id, "patients": patients}

    def ndjson_iter(self) -> Iterable[Dict[str, Any]]:
        for p in self.patients:
            d = export_leaf_object(p, exclude=set())
            d.setdefault("trial_id", self.trial_id)
            yield d

    def to_dataframe_wide(self, prefix_sep="."):
        patient_cls = type(next(iter(self.patients), object()))
        df_nested = build_nested_df(self.patients, patient_cls)
        return to_wide(df_nested, prefix_sep)

    def to_frames_normalized(self, **_):
        patient_cls = type(next(iter(self.patients), object()))
        df_nested = build_nested_df(self.patients, patient_cls)
        return to_normalized(df_nested)

    def __iter__(self):
        return iter(self.patients)

    def __getitem__(self, item):
        return self.patients[item]
