from abc import ABC, abstractmethod
from typing import Any, Mapping, ClassVar, Self

from omop_etl.harmonization.core.track_validated import TrackedValidated


class DomainBase(TrackedValidated, ABC):
    """
    Base class for all domain models with schema contract support.

    Subclasses must define:
    - `class Fields:` with string constants for canonical field names (wire schema)

    Subclasses may optionally define:
    - `MATERIAL_FIELDS` tuple referencing Fields constants for materiality filtering
    """

    # internal cache, use data_fields() method to access
    _data_fields: ClassVar[tuple[str, ...] | None] = None
    _schema_validated: ClassVar[bool] = False

    # optional
    MATERIAL_FIELDS: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def __init__(self, patient_id: str) -> None:
        """Initialize domain object with patient_id. Must be implemented by subclasses."""
        ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # reset cache for each subclass
        cls._data_fields = None
        cls._schema_validated = False

    @classmethod
    def _derive_data_fields(cls) -> tuple[str, ...]:
        """Derive data fields from Fields inner class string constants."""
        fields_cls = getattr(cls, "Fields", None)
        if fields_cls is None:
            raise ValueError(f"{cls.__name__}: must define a Fields inner class")
        out: list[str] = []
        for name, value in vars(fields_cls).items():
            if name.startswith("_"):
                continue
            if isinstance(value, str):
                out.append(value)
        return tuple(out)

    @classmethod
    def _ensure_schema(cls) -> None:
        """Lazily derive and validate schema on first access."""
        if cls._schema_validated:
            return

        if cls._data_fields is None:
            cls._data_fields = cls._derive_data_fields()

        fields = cls._data_fields
        if not fields:
            raise ValueError(f"{cls.__name__}: Fields must define at least one string constant")

        if len(fields) != len(set(fields)):
            raise ValueError(f"{cls.__name__}.data_fields has duplicates")

        material = set(cls.MATERIAL_FIELDS)
        if material and not material.issubset(set(fields)):
            raise ValueError(f"{cls.__name__}.MATERIAL_FIELDS not subset of data_fields: {material - set(fields)}")

        # validate every Fields value matches an actual property on the class
        fields_cls = getattr(cls, "Fields", None)
        if fields_cls is not None:
            for const_name, field_value in vars(fields_cls).items():
                if const_name.startswith("_") or not isinstance(field_value, str):
                    continue
                if not hasattr(cls, field_value):
                    raise TypeError(f"{cls.__name__}.Fields.{const_name} = {field_value!r} but {cls.__name__} has no property '{field_value}'")

        cls._schema_validated = True

    @classmethod
    def data_fields(cls) -> tuple[str, ...]:
        """Returns the data fields for this domain class."""
        cls._ensure_schema()
        return cls._data_fields

    @classmethod
    def from_row(
        cls,
        patient_id: str,
        row: Mapping[str, Any],
    ) -> Self:
        """
        Construct instance from a row dict.
        Assumes schema validation already happened at DataFrame level.
        """
        obj = cls(patient_id)
        for field in cls.data_fields():
            setattr(obj, field, row[field])
        return obj
