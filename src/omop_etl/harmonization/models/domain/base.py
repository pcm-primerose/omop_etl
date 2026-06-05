from abc import ABC, abstractmethod
from typing import Any, Mapping, ClassVar, Self

from omop_etl.harmonization.core.track_validated import TrackedValidated


class DomainBase(TrackedValidated, ABC):
    """
    Base class for all domain models with schema contract support.

    Subclasses must define:
    - `class Fields:` with string constants for canonical field names (schema from processor to domain)

    Subclasses can optionally define:
    - `REQUIRED_FIELDS` tuple referencing Fields constants, a record is real if all fields present.
    - `NATURAL_KEY_FIELDS` tuple referencing Fields that make up the natural key
      (identity, for dedup and FK linkage).

    REQUIRED_FIELDS and NATURAL_KEY_FIELDS are independent: they usually overlap
    (the subject is part of the identity) but may diverge when identity is
    positional/temporal and the subject is content.
    """

    # internal cache, use data_fields() method to access
    _data_fields: ClassVar[tuple[str, ...] | None] = None
    _schema_validated: ClassVar[bool] = False

    # collection and singleton subclasses override
    REQUIRED_FIELDS: ClassVar[tuple[str, ...]] = ()
    NATURAL_KEY_FIELDS: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def __init__(self, patient_id: str) -> None:  # noqa
        """Initialize domain object with patient_id. Must be implemented by subclasses."""
        ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # reset cache for each subclass
        cls._data_fields = None
        cls._schema_validated = False
        # validate the membership contract eagerly at class-definition (import) time.
        # property-existence check is deferred to _validate_membership,
        # since some subclasses (C30, EQ5D) generate fields outside class
        if getattr(cls, "Fields", None) is not None:
            cls._validate_membership()

    def natural_key(self) -> tuple:
        return tuple(getattr(self, f) for f in self.NATURAL_KEY_FIELDS)

    def required_fields(self) -> tuple:
        return tuple(getattr(self, f) for f in self.REQUIRED_FIELDS)

    def sort_key(self) -> tuple:
        """None-safe sort key derived from natural_key, None values sort last."""
        return tuple((v is None, v) for v in self.natural_key())

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
    def _validate_membership(cls) -> None:
        """
        Structural and membership contract:
        Fields non-empty, no duplicates and
        `REQUIRED_FIELDS` and `NATURAL_KEY_FIELDS` subsets of `data_fields()`.

        Does not check that each Fields value names a property: subclasses like
        C30/EQ5D generate their fields after the class body, so those properties
        don't exist yet at `__init_subclass__` time, deferred to
        `_ensure_schema` using `data_fields()`.
        """
        if cls._data_fields is None:
            cls._data_fields = cls._derive_data_fields()

        fields = cls._data_fields
        if not fields:
            raise ValueError(f"{cls.__name__}: Fields must define at least one string constant")

        if len(fields) != len(set(fields)):
            raise ValueError(f"{cls.__name__}.data_fields has duplicates")

        field_set = set(fields)

        required = set(cls.REQUIRED_FIELDS)
        if required and not required.issubset(field_set):
            raise ValueError(f"{cls.__name__}.REQUIRED_FIELDS not a subset of data_fields: {required - field_set}")

        natural_key = set(cls.NATURAL_KEY_FIELDS)
        if natural_key and not natural_key.issubset(field_set):
            raise ValueError(f"{cls.__name__}.NATURAL_KEY_FIELDS not a subset of data_fields: {natural_key - field_set}")

    @classmethod
    def _ensure_schema(cls) -> None:
        """
        Full schema validation (idempotent). Runs `_validate_membership` (also run
        eagerly at import) plus the property-existence check: every Fields value must
        name an actual property on the class. Triggered lazily via `data_fields()`,
        i.e. after any post-class-body property generation has run.
        """
        if cls._schema_validated:
            return

        cls._validate_membership()

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
        fields = cls._data_fields
        assert fields is not None, f"{cls.__name__}._ensure_schema() did not populate _data_fields"
        return fields

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
