from abc import ABC, abstractmethod
from typing import Any, Mapping, ClassVar, Self

from omop_etl.harmonization.core.track_validated import TrackedValidated


class DomainBase(TrackedValidated, ABC):
    """
    Base class for all domain models with schema contract support.

    data_fields() is derived lazily by scanning for public writable properties,
    to support classes that dynamically generate properties after class definition.
    """

    # internal cache, use data_fields() method to access
    _data_fields: ClassVar[tuple[str, ...] | None] = None
    _schema_validated: ClassVar[bool] = False

    # optional overrides
    MATERIAL_COLS: ClassVar[tuple[str, ...]] = ()
    EXCLUDE_DATA_FIELDS: ClassVar[frozenset[str]] = frozenset({"updated_fields", "patient_id"})

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
        """Scan class for all public writable properties."""
        out: list[str] = []
        for name in dir(cls):
            if name.startswith("_") or name in cls.EXCLUDE_DATA_FIELDS:
                continue
            attr = getattr(cls, name, None)
            if isinstance(attr, property) and attr.fset is not None:
                out.append(name)
        return tuple(sorted(out))

    @classmethod
    def _ensure_schema(cls) -> None:
        """Lazily derive and validate schema on first access."""
        if cls._schema_validated:
            return

        # derive expected fields from properties
        if cls._data_fields is None:
            cls._data_fields = cls._derive_data_fields()

        fields = cls._data_fields
        if not fields:
            raise ValueError(f"{cls.__name__}: no data fields derived (no public writable properties found)")

        if len(fields) != len(set(fields)):
            raise ValueError(f"{cls.__name__}.data_fields has duplicates")

        material = set(cls.MATERIAL_COLS)
        if not material.issubset(set(fields)):
            raise ValueError(f"{cls.__name__}.MATERIAL_COLS not subset of data_fields: {material - set(fields)}")

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
