from unittest.mock import Mock
import polars as pl
import pytest
from omop_etl.preprocessing.core.registry import (
    register_trial,
    get_registered_trials,
    get_processor,
    clear_registry,
    TRIAL_PROCESSORS,
)


@pytest.fixture
def clean_registry():
    """Clean registry before each test"""
    original_processors = TRIAL_PROCESSORS.copy()
    clear_registry()
    yield
    # restore original processors
    TRIAL_PROCESSORS.clear()
    TRIAL_PROCESSORS.update(original_processors)


def test_config_and_processor_trials_match():
    """Ensure every available config has a corresponding processor"""
    # real registered trials
    from omop_etl.preprocessing.core.registry import TRIAL_PROCESSORS
    from omop_etl.preprocessing.core.config_loader import available_trials

    available_configs = set(available_trials())
    available_processors = set(TRIAL_PROCESSORS.keys())

    # every config should have a processor
    missing_processors = available_configs - available_processors
    assert not missing_processors, f"Configs without processors: {missing_processors}"

    # every processor should have a configs
    missing_configs = available_processors - available_configs
    assert not missing_configs, f"Processors without configs: {missing_configs}"


def test_register_trial(clean_registry):
    @register_trial("test_trial")
    def dummy_processor(df, ecfg, opts):
        return df

    assert "test_trial" in get_registered_trials()
    assert get_processor("test_trial") == dummy_processor


def test_duplicate_registration_fails(clean_registry):
    @register_trial("test_trial")
    def processor1(df, ecfg, opts):
        return df

    with pytest.raises(KeyError, match="Processor 'test_trial' already registered"):

        @register_trial("test_trial")
        def processor2(df, ecfg, opts):
            return df


def test_get_processor_missing_trial(clean_registry):
    with pytest.raises(KeyError, match="No processor for trial 'missing'"):
        get_processor("missing")


def test_get_registered_trials_empty(clean_registry):
    assert get_registered_trials() == []


def test_clear_registry(clean_registry):
    @register_trial("test_trial")
    def processor(df, ecfg, opts):
        return df

    assert len(get_registered_trials()) == 1

    clear_registry()

    assert len(get_registered_trials()) == 0
    assert get_registered_trials() == []


def test_register_trial_preserves_function(clean_registry):
    """Test that the decorator returns the original function unchanged"""

    def original_processor(df, ecfg, opts):
        """Original docstring"""
        return df.with_columns(pl.lit("test").alias("test_col"))

    decorated = register_trial("test_trial")(original_processor)

    # func should be unchanged
    assert decorated == original_processor
    assert decorated.__name__ == "original_processor"
    assert decorated.__doc__ == "Original docstring"

    # but registered
    assert get_processor("test_trial") == original_processor


def test_processor_actually_callable(clean_registry):
    @register_trial("test_trial")
    def processor(df, ecfg, opts):
        return df.with_columns(pl.lit("processed").alias("status"))

    test_df = pl.DataFrame({"id": [1, 2, 3]})
    mock_ecfg = Mock()
    mock_opts = Mock()

    # get and call processor
    proc = get_processor("test_trial")
    result = proc(test_df, mock_ecfg, mock_opts)

    assert "status" in result.columns
    assert result["status"].unique().to_list() == ["processed"]


def test_registry_state_after_error(clean_registry):
    @register_trial("good_trial")
    def good_processor(df, ecfg, opts):
        return df

    # duplicate registration should fail
    with pytest.raises(KeyError):

        @register_trial("good_trial")
        def bad_processor(df, ecfg, opts):
            return df

    # original registration should still be intact
    assert "good_trial" in get_registered_trials()
    assert get_processor("good_trial") == good_processor


def test_register_different_function_types(clean_registry):
    """Test registering different types of callables"""

    # function
    @register_trial("function_trial")
    def func_processor(df, ecfg, opts):
        return df

    _ = register_trial("lambda_trial")(lambda df, ecfg, opts: df)

    # class method
    class ProcessorClass:
        @staticmethod
        def process(df, ecfg, opts):
            return df

    _ = register_trial("class_trial")(ProcessorClass.process)

    trials = get_registered_trials()
    assert "function_trial" in trials
    assert "lambda_trial" in trials
    assert "class_trial" in trials

    # all should be callable
    assert callable(get_processor("function_trial"))
    assert callable(get_processor("lambda_trial"))
    assert callable(get_processor("class_trial"))


def test_case_insensitive_duplicate_prevention(clean_registry):
    @register_trial("trial_name")
    def processor1(df, ecfg, opts):
        return df

    # duplicates:
    with pytest.raises(KeyError, match="already registered"):

        @register_trial("TRIAL_NAME")
        def processor2(df, ecfg, opts):
            return df

    with pytest.raises(KeyError, match="already registered"):

        @register_trial("Trial_Name")
        def processor3(df, ecfg, opts):
            return df


def test_case_insensitive_registration(clean_registry):
    @register_trial("Test_Trial")
    def processor(df, ecfg, opts):
        return df

    assert get_processor("test_trial") == processor
    assert get_processor("TEST_TRIAL") == processor
    assert get_processor("Test_Trial") == processor
    assert get_processor("TeSt_TrIaL") == processor
