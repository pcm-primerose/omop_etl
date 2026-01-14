from typing import Any, Mapping, ClassVar, Self

from omop_etl.harmonization.core.track_validated import TrackedValidated


class DomainBase(TrackedValidated):
    """Base class for all domain models with schema contract support."""

    # Explicit schema contract - subclasses must define
    CANONICAL_COLS: ClassVar[tuple[str, ...]] = ()

    # Optional: columns that should trigger warnings/metrics if null (for observability)
    SOFT_REQUIRED_COLS: ClassVar[tuple[str, ...]] = ()

    # Optional: columns that determine "materiality" - if all are null, row is non-material
    # Processors use this: .filter(pl.any_horizontal(pl.col(*Domain.MATERIAL_COLS).is_not_null()))
    MATERIAL_COLS: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate schema contract at class definition time."""
        super().__init_subclass__(**kwargs)

        # Skip validation for intermediate base classes
        if cls.CANONICAL_COLS == ():
            return

        # No duplicates in CANONICAL_COLS
        cols = cls.CANONICAL_COLS
        if len(cols) != len(set(cols)):
            dupes = [c for c in cols if cols.count(c) > 1]
            raise ValueError(f"{cls.__name__}.CANONICAL_COLS has duplicates: {set(dupes)}")

        # SOFT_REQUIRED_COLS ⊆ CANONICAL_COLS
        soft = set(cls.SOFT_REQUIRED_COLS)
        canonical = set(cols)
        if not soft.issubset(canonical):
            invalid = soft - canonical
            raise ValueError(f"{cls.__name__}.SOFT_REQUIRED_COLS contains columns not in CANONICAL_COLS: {invalid}")

        # MATERIAL_COLS ⊆ CANONICAL_COLS
        material = set(cls.MATERIAL_COLS)
        if not material.issubset(canonical):
            invalid = material - canonical
            raise ValueError(f"{cls.__name__}.MATERIAL_COLS contains columns not in CANONICAL_COLS: {invalid}")

        # MATERIAL_COLS must be non-empty for concrete domains
        if not cls.MATERIAL_COLS:
            raise ValueError(f"{cls.__name__}.MATERIAL_COLS is empty. Concrete domains must define at least one material column.")

    @classmethod
    def expected_columns(cls) -> tuple[str, ...]:
        """Returns the canonical columns this class expects from a DataFrame row."""
        return cls.CANONICAL_COLS

    @classmethod
    def from_row(
        cls,
        patient_id: str,
        row: Mapping[str, Any],
        *,
        context: Mapping[str, Any] | None = None,
    ) -> Self:
        """
        Construct instance from a row dict.
        Assumes schema validation already happened at DataFrame level.

        Sets field to whatever is in the row (including None).
        Setters handle validation/coercion.

        Args:
            patient_id: The patient identifier
            row: Dict-like row from DataFrame iteration
            context: Optional metadata (source info, flags, etc.) for future use
        """
        obj = cls(patient_id)
        for col in cls.CANONICAL_COLS:
            setattr(obj, col, row[col])  # Fail-fast: schema guarantees col exists
        return obj
