from collections.abc import Sequence
from dataclasses import dataclass, fields, is_dataclass
from typing import ClassVar, get_origin, get_type_hints, Literal

from omop_etl.omop.core.linkage import PolymorphicTarget, polymorphic_row_key
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.vocabulary import Vocabulary

# Checks are derived from cdm5.5_primary_keys.sql,
# but run after all builders so we raise at ETL-time instead of in DB after loading

# CDM convention is to name a table's PK column by its table name,
# e.g. person_id from the PERSON table.
# `PK_COLUMN_OVERRIDES` are for tables that don't follow this convention.
PK_COLUMN_OVERRIDES = {OmopTables.DEATH: "person_id"}


def _pk_column(table: str) -> str:
    """The table's PK column name, applying PK_COLUMN_OVERRIDES."""
    return PK_COLUMN_OVERRIDES.get(table, f"{table}_id")


# Tables without a "<table>_id" column to check as PKs.
# These are checed using `COMPOSITE_KEY_FIELDS` instead,
# except for CDM_SOURCE which is a singleton without a NK.
PK_UNIQUENESS_EXEMPT = frozenset({OmopTables.COHORT, OmopTables.EPISODE_EVENT, OmopTables.CDM_SOURCE})


# Composite NK-uniqueness for non-nullable fields in the tables without PK columns
COMPOSITE_KEY_FIELDS = {
    OmopTables.COHORT: ("cohort_definition_id", "subject_id", "cohort_start_date"),
    OmopTables.EPISODE_EVENT: ("episode_id", "event_id", "episode_event_field_concept_id"),
}


# Tables that don't follow the "<table>_id" pattern for FK columns
FK_TARGET_TABLE_OVERRIDES = {
    "episode_parent_id": OmopTables.EPISODE,
    "preceding_visit_occurrence_id": OmopTables.VISIT_OCCURRENCE,
    "subject_id": OmopTables.PERSON,
}


def _fk_target_table(field: str) -> str:
    """The table an FK column references, applying FK_TARGET_TABLE_OVERRIDES."""
    return FK_TARGET_TABLE_OVERRIDES.get(field, field.removesuffix("_id"))


# FK columns that don't have a static target table,
# these are passed through `BuildContext` instead of being derived from field names
POLYMORPHIC_FK_FIELDS = frozenset({"event_id", "measurement_event_id", "observation_event_id"})

_IntegrityViolationKind = Literal[
    "duplicate_pk",
    "orphan_fk",
    "orphan_concept",
    "nul_byte",
    "polymorphic_target_collision",
]


@dataclass(frozen=True, slots=True)
class _IntegrityViolation:
    table: str
    kind: _IntegrityViolationKind
    detail: str


class PreLoadIntegrityError(RuntimeError):
    """
    Raised by `validate_integrity` when the assembled `OmopTables` fails PK
    uniqueness or FK/concept referential integrity.
    """

    def __init__(self, violations: list[_IntegrityViolation]):
        self.violations = violations
        detail = "\n".join(f"  - [{v.table}] {v.kind}: {v.detail}" for v in violations)
        super().__init__(f"{len(violations)} integrity violation(s) found before load:\n{detail}")


def validate_integrity(
    tables: OmopTables,
    vocabulary: Vocabulary,
    polymorphic_targets: Sequence[PolymorphicTarget] = (),
) -> None:
    """
    PK uniqueness and FK/concept referential integrity over the assembled
    `OmopTables`, collecting every violation before raising one `PreLoadIntegrityError`.

    `polymorphic_targets` carries which table that value targets, for each row with a polymorphic event-link
    field set (see POLYMORPHIC_FK_FIELDS), recorded by `BuildContext.record_polymorphic_target`
    when the field is set. A set polymorphic field with no matching entry here is a violation.
    """
    violations: list[_IntegrityViolation] = []
    pk_values_by_table = {table: _pk_values(tables, table) for table in OmopTables.values() if table not in PK_UNIQUENESS_EXEMPT}
    targets_by_key, target_collisions = _polymorphic_targets_by_key(polymorphic_targets)
    violations.extend(target_collisions)

    for table in OmopTables.values():
        rows = tables.get(table)
        if not rows:
            continue

        if table in COMPOSITE_KEY_FIELDS:
            violations.extend(_duplicate_composite_key_violations(table, COMPOSITE_KEY_FIELDS[table], rows))
        elif table not in PK_UNIQUENESS_EXEMPT:
            violations.extend(_duplicate_pk_violations(table, _pk_column(table), rows))

        violations.extend(_nul_byte_violations(table, rows))

        for field in _id_fields(rows[0]):
            if field in POLYMORPHIC_FK_FIELDS:
                violations.extend(_polymorphic_fk_violations(table, field, rows, pk_values_by_table, targets_by_key))
                continue
            if field.endswith("_concept_id"):
                violations.extend(_orphan_concept_violations(table, field, rows, vocabulary))
                continue
            # a table's table-id column targets itself, except for DEATH,
            # which uses person_id as it's PK but that's also a readl FK into PERSON,
            # so this must be checked:
            target = _fk_target_table(field)
            if target not in pk_values_by_table:
                continue
            violations.extend(_orphan_fk_violations(table, field, rows, target, pk_values_by_table[target]))

    if violations:
        raise PreLoadIntegrityError(violations)


def _pk_values(tables: OmopTables, table: str) -> set[int]:
    return {getattr(row, _pk_column(table)) for row in tables.get(table, [])}


def _duplicate_pk_violations(table: str, pk_field: str, rows: list[object]) -> list[_IntegrityViolation]:
    counts: dict[int, int] = {}
    for row in rows:
        value = getattr(row, pk_field)
        counts[value] = counts.get(value, 0) + 1
    return [_IntegrityViolation(table, "duplicate_pk", f"{pk_field}={value} appears {count} times") for value, count in counts.items() if count > 1]


def _duplicate_composite_key_violations(table: str, key_fields: tuple[str, ...], rows: list[object]) -> list[_IntegrityViolation]:
    counts: dict[tuple[object, ...], int] = {}
    for row in rows:
        key = tuple(getattr(row, f) for f in key_fields)
        counts[key] = counts.get(key, 0) + 1
    return [_IntegrityViolation(table, "duplicate_pk", f"({', '.join(key_fields)})={key} appears {count} times") for key, count in counts.items() if count > 1]


def _polymorphic_targets_by_key(
    polymorphic_targets: Sequence[PolymorphicTarget],
) -> tuple[dict[tuple[str, tuple[object, ...], str], str], list[_IntegrityViolation]]:
    """
    Index `polymorphic_targets` by (table, row_key, field).
    `row_key` is only collision-free by construction for measurement/observation (it's their
    own RowIdGenerator-checked PK), for episode_event it also depends on the target's id,
    from a different namespace RowIdGenerator never cross- checks.
    So two different targets having the same key is possible, and reproted if occurring
    instead of overwriting.
    """
    by_key: dict[tuple[str, tuple[object, ...], str], str] = {}
    violations: list[_IntegrityViolation] = []
    for pt in polymorphic_targets:
        key = (pt.table, pt.row_key, pt.field)
        existing = by_key.get(key)
        if existing is not None and existing != pt.target_table:
            violations.append(
                _IntegrityViolation(
                    pt.table,
                    "polymorphic_target_collision",
                    f"{pt.field} on row {pt.row_key} recorded with two different target tables: {existing!r} and {pt.target_table!r}",
                )
            )
            continue
        by_key[key] = pt.target_table
    return by_key, violations


def _polymorphic_fk_violations(
    table: str,
    field: str,
    rows: list[object],
    pk_values_by_table: dict[str, set[int]],
    targets_by_key: dict[tuple[str, tuple[object, ...], str], str],
) -> list[_IntegrityViolation]:
    violations: list[_IntegrityViolation] = []
    for row in rows:
        value = getattr(row, field)
        if value is None:
            continue
        row_key = polymorphic_row_key(table, row)
        target_table = targets_by_key.get((table, row_key, field))
        if target_table is None:
            violations.append(_IntegrityViolation(table, "orphan_fk", f"{field}={value} has no recorded polymorphic target table"))
            continue
        target_pks = pk_values_by_table.get(target_table)
        if target_pks is not None and value not in target_pks:
            violations.append(_IntegrityViolation(table, "orphan_fk", f"{field}={value} not found in {target_table}"))
    return violations


def _nul_byte_violations(table: str, rows: list[object]) -> list[_IntegrityViolation]:
    """Postgres text/varchar columns reject an embedded NUL byte outright, at any length."""
    return [
        _IntegrityViolation(table, "nul_byte", f"{f.name} contains a NUL byte (0x00), which Postgres text columns cannot store")
        for row in rows
        if is_dataclass(row)
        for f in fields(row)
        if isinstance(value := getattr(row, f.name), str) and "\x00" in value
    ]


def _orphan_fk_violations(table: str, field: str, rows: list[object], target: str, target_pks: set[int]) -> list[_IntegrityViolation]:
    return [
        _IntegrityViolation(table, "orphan_fk", f"{field}={value} not found in {target}")
        for row in rows
        if (value := getattr(row, field)) is not None and value not in target_pks
    ]


def _orphan_concept_violations(table: str, field: str, rows: list[object], vocabulary: Vocabulary) -> list[_IntegrityViolation]:
    return [
        _IntegrityViolation(table, "orphan_concept", f"{field}={value} not in vocabulary")
        for row in rows
        if (value := getattr(row, field)) is not None and vocabulary.hydrate(value) is None
    ]


def _id_fields(row: object) -> tuple[str, ...]:
    if not is_dataclass(row) or isinstance(row, type):
        raise TypeError(f"Expected a dataclass instance, got: {type(row)}")

    hints = get_type_hints(type(row), include_extras=True)
    return tuple(f.name for f in fields(row) if not f.name.startswith("_") and get_origin(hints.get(f.name)) is not ClassVar and f.name.endswith("_id"))
