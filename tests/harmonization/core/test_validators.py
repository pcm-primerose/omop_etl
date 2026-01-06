import datetime as dt
import pytest

from omop_etl.harmonization.core.validators import StrictValidators as va


@pytest.mark.parametrize("val", [None, "a", ""])
def test_validate_optional_str_ok(val):
    assert va.validate_optional_str(val, "name") is val


@pytest.mark.parametrize("val", [None, 1, -2, 0])
def test_validate_optional_int_ok(val):
    if val is None:
        assert va.validate_optional_int(val, "age") is None
    else:
        assert va.validate_optional_int(val, "age") == val


@pytest.mark.parametrize("val", [None, 1.0, -2.5, 0.0])
def test_validate_optional_float_ok(val):
    if val is None:
        assert va.validate_optional_float(val, "score") is None
    else:
        assert va.validate_optional_float(val, "score") == val


@pytest.mark.parametrize("val", [None, dt.date(2020, 1, 1)])
def test_validate_optional_date_ok(val):
    assert va.validate_optional_date(val, "dod") is val


@pytest.mark.parametrize("val", [None, True, False])
def test_validate_optional_bool_ok(val):
    assert va.validate_optional_bool(val, "flag") is val


@pytest.mark.parametrize("bad", [1, 1.0, True, dt.date(2020, 1, 1), object()])
def test_validate_optional_str_err(bad):
    with pytest.raises(TypeError):
        va.validate_optional_str(bad, "name")


@pytest.mark.parametrize("bad", ["1", 1.0, True, None.__class__])  # type: ignore
def test_validate_optional_int_err(bad):
    if bad is True:
        with pytest.raises(TypeError):
            va.validate_optional_int(bad, "age")
    elif bad == "1" or bad == 1.0 or bad is None.__class__:  # type: ignore
        with pytest.raises(TypeError):
            va.validate_optional_int(bad, "age")


@pytest.mark.parametrize("bad", ["1.0", 1, True, dt.date(2020, 1, 1)])
def test_validate_optional_float_err(bad):
    with pytest.raises(TypeError):
        va.validate_optional_float(bad, "score")


@pytest.mark.parametrize("bad", ["2020-01-01", 1, 1.0, True, dt.datetime(2020, 1, 1, 0, 0)])
def test_validate_optional_date_err(bad):
    with pytest.raises(TypeError):
        va.validate_optional_date(bad, "dod")


@pytest.mark.parametrize("bad", ["true", 1, 0, dt.date(2020, 1, 1)])
def test_validate_optional_bool_err(bad):
    with pytest.raises(TypeError):
        va.validate_optional_bool(bad, "flag")
