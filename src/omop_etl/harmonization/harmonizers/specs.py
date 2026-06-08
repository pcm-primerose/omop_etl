from dataclasses import dataclass
from typing import Literal, Callable, Any, TypeVar, TYPE_CHECKING
import polars as pl

from omop_etl.harmonization.models.domain.base import DomainBase

if TYPE_CHECKING:
    # import-only: base imports specs at runtime, runtime import here would be circular
    from omop_etl.harmonization.harmonizers.base import BaseHarmonizer

# processor function type: takes harmonizer instance, returns DataFrame or None
type ProcessorFn = Callable[[BaseHarmonizer], pl.DataFrame | None]

# lets @scalar/@singleton/@collection decorators return the original function type
_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(frozen=True)
class SpecBase:
    """Base class for all processor specs with common fields."""

    name: str
    process: ProcessorFn
    strict_schema: bool | None = None
    skip_missing_patients: bool = False
    subject_col: str = "SubjectId"


@dataclass(frozen=True)
class ScalarSpec(SpecBase):
    """Spec for scalar patient attributes (e.g., cohort_name, sex, age)."""

    kind: Literal["scalar"] = "scalar"
    target_attr: str = ""
    value_col: str = ""
    on_duplicate: Literal["error", "first", "last"] = "error"


@dataclass(frozen=True, kw_only=True)
class SingletonSpec(SpecBase):
    """Spec for singleton domain objects (one per patient)."""

    target_domain: type[DomainBase]
    kind: Literal["singleton"] = "singleton"
    on_duplicate: Literal["error", "first", "last"] = "error"


@dataclass(frozen=True, kw_only=True)
class CollectionSpec(SpecBase):
    """Spec for collection domain objects (multiple per patient)."""

    target_domain: type[DomainBase]
    kind: Literal["collection"] = "collection"
    mode: Literal["replace", "extend"] = "replace"
    order_by: tuple[str, ...] = ()
    require_order_by: bool = False
    items_col: str = "items"
    on_natural_key_conflict: Literal["error", "warn"] = "warn"


# union type for all specs
ProcessorSpec = ScalarSpec | SingletonSpec | CollectionSpec


# sentinel attribute name used to attach a spec to a decorated processor method.
# __init_subclass__ on BaseHarmonizer scans class attributes for this marker and
# collects the specs into the subclass's SPECS tuple in declaration order.
_SPEC_ATTR = "__processor_spec__"


def _derived_name(fn: Callable[..., Any]) -> str:
    """Strip the conventional `_process_` prefix to get the logical spec name."""
    name: str = getattr(fn, "__name__")
    return name.removeprefix("_process_")


def scalar(
    *,
    name: str | None = None,
    target_attr: str | None = None,
    value_col: str | None = None,
    on_duplicate: Literal["error", "first", "last"] = "error",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a scalar processor.

    By default, `name`, `target_attr`, and `value_col` are derived from the method
    name with the `_process_` prefix stripped, so `_process_sex` produces a spec
    with name="sex" / target_attr="sex" / value_col="sex". Provide explicit kwargs
    only when the method name doesn't match the desired Patient attribute or value
    column.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = ScalarSpec(
            name=name or derived,
            process=fn,
            target_attr=target_attr or derived,
            value_col=value_col or derived,
            on_duplicate=on_duplicate,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator


def singleton(
    target_domain: type[DomainBase],
    *,
    name: str | None = None,
    on_duplicate: Literal["error", "first", "last"] = "error",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a singleton-domain processor.

    `target_domain` is required. `name` defaults to the method name with the
    `_process_` prefix stripped.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = SingletonSpec(
            name=name or derived,
            process=fn,
            target_domain=target_domain,
            on_duplicate=on_duplicate,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator


def collection(
    target_domain: type[DomainBase],
    *,
    name: str | None = None,
    mode: Literal["replace", "extend"] = "replace",
    order_by: tuple[str, ...] = (),
    require_order_by: bool = False,
    items_col: str = "items",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
    on_natural_key_conflict: Literal["error", "warn"] = "warn",
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a collection-domain processor.

    `target_domain` is required. `name` defaults to the method name with the
    `_process_` prefix stripped.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = CollectionSpec(
            name=name or derived,
            process=fn,
            target_domain=target_domain,
            mode=mode,
            order_by=order_by,
            require_order_by=require_order_by,
            items_col=items_col,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
            on_natural_key_conflict=on_natural_key_conflict,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator
