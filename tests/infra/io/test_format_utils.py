import pytest

from omop_etl.infra.io.format_utils import ext, expand_formats, normalize_format


@pytest.fixture
def allowed_tabular():
    return "csv", "tsv", "parquet"


@pytest.fixture
def allowed_wide():
    return "csv", "tsv", "parquet", "json"


def test_ext_valid():
    assert ext("csv") == ".csv"
    assert ext("tsv") == ".tsv"
    assert ext("parquet") == ".parquet"
    assert ext("json") == ".json"


def test_ext_invalid_raises():
    with pytest.raises(ValueError, match="Unknown format: xml"):
        ext("xml")  # type: ignore


def test_expand_formats_simple_and_casefold(allowed_tabular):
    assert expand_formats("csv", allowed=allowed_tabular) == ["csv"]
    assert expand_formats(["CSV", "parquet"], allowed=allowed_tabular) == ["csv", "parquet"]  # type: ignore


def test_expand_formats_aliases(allowed_tabular):
    assert expand_formats("txt", allowed=allowed_tabular) == ["tsv"]  # type: ignore


def test_expand_formats_nested_lists(allowed_tabular):
    # nested input should flatten
    assert expand_formats([["csv"], ["parquet"]], allowed=allowed_tabular) == ["csv", "parquet"]  # type: ignore
    assert expand_formats([["csv", "tsv"], ["csv"]], allowed=allowed_tabular) == ["csv", "tsv"]  # type: ignore


def test_expand_formats_dedup_keeps_first_seen_order(allowed_tabular):
    # duplicates removed, order preserved by first appearance
    assert expand_formats(["parquet", "csv", "csv", "tsv"], allowed=allowed_tabular) == ["parquet", "csv", "tsv"]


def test_expand_formats_all_token_expands_in_allowed_order(allowed_wide):
    # "all" expands to allowed in that order
    assert expand_formats("all", allowed=allowed_wide) == list(allowed_wide)


def test_expand_formats_invalid_rejected(allowed_tabular):
    with pytest.raises(ValueError, match=r"Unsupported format: xml.*Allowed: .* or 'all'"):
        expand_formats("xml", allowed=allowed_tabular)  # type: ignore


def test_normalize_format_defaults_to_wide_formats():
    assert normalize_format("CSV") == "csv"  # type: ignore
    assert normalize_format("txt") == "tsv"  # type: ignore


def test_normalize_format_with_custom_allowed(allowed_tabular):
    # custom allowed list is respected
    assert normalize_format("parquet", allowed=allowed_tabular) == "parquet"  # type: ignore
    with pytest.raises(ValueError):
        normalize_format("json", allowed=allowed_tabular)  # type: ignore


def test_normalize_raises_on_unknown():
    with pytest.raises(ValueError):
        normalize_format("xlsx")  # type: ignore
