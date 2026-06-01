from typing import Set

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class Cohort(DomainBase):
    """
    The trial cohort a patient was allocated to. One per patient (singleton).

    Trials name cohorts differently for semantically identical groups, so the
    cohort is stored both verbatim (`raw_name`, the source's composed string)
    and harmonized into parts: `target_biomarker`, `cancer_type`, and `drugs`.
    `normalized_name` is composed from the harmonized parts and is the value
    used for pipeline-level cohort filtering and OMOP cohort / cohort_definition.

    Unmapped raw values keep `normalized_name=None` (and whichever parts did
    resolve) rather than being dropped, so the audit trail is preserved.
    """

    class Fields:
        RAW_NAME = "raw_name"
        NORMALIZED_NAME = "normalized_name"
        TARGET_BIOMARKER = "target_biomarker"
        CANCER_TYPE = "cancer_type"
        DRUGS = "drugs"

    # raw_name is always present, normalized_name can be
    # None when unmapped, so it can't anchor identity.
    REQUIRED_FIELDS = (Fields.RAW_NAME,)
    NATURAL_KEY_FIELDS = (Fields.RAW_NAME,)

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._raw_name: str | None = None
        self._normalized_name: str | None = None
        self._target_biomarker: str | None = None
        self._cancer_type: str | None = None
        self._drugs: tuple[str, ...] = ()
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def raw_name(self) -> str | None:
        return self._raw_name

    @raw_name.setter
    def raw_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.raw_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def normalized_name(self) -> str | None:
        return self._normalized_name

    @normalized_name.setter
    def normalized_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.normalized_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def target_biomarker(self) -> str | None:
        return self._target_biomarker

    @target_biomarker.setter
    def target_biomarker(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_biomarker,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def cancer_type(self) -> str | None:
        return self._cancer_type

    @cancer_type.setter
    def cancer_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cancer_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def drugs(self) -> tuple[str, ...]:
        return self._drugs

    @drugs.setter
    def drugs(self, value) -> None:
        self._set_validated_prop(
            prop=self.__class__.drugs,
            value=value,
            validator=StrictValidators.validate_optional_str_tuple,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"raw_name={self.raw_name!r}{delim} "
            f"normalized_name={self.normalized_name!r}{delim} "
            f"target_biomarker={self.target_biomarker!r}{delim} "
            f"cancer_type={self.cancer_type!r}{delim} "
            f"drugs={self.drugs!r}"
            f")"
        )
