import polars as pl

from omop_etl.scripts.semantic_extractor import add_term_id
from src.omop_etl.scripts.semantic_extractor import (
    frames_to_dict,
    dict_to_counts,
    SheetData,
    EcrfConfig,
)


def make_cfg():
    coh = pl.DataFrame(
        {
            "SubjectId": ["A", "B", "C", "D"],
            "ICD10COD": ["C50", "C50", "C18", None],
        },
    )
    tr = pl.DataFrame(
        {
            "SubjectId": ["A", "B", "C"],
            "TRNAME": ["DrugA", "DrugA", "DrugB"],
        },
    )

    return EcrfConfig(configs=[], data=[SheetData(key="coh", data=coh), SheetData(key="tr", data=tr)])


def test_frames_to_dict():
    cfg = make_cfg()
    d = frames_to_dict(cfg)
    assert set(d.keys()) == {"COH_ICD10COD", "TR_TRNAME"}
    assert d["COH_ICD10COD"].name == "ICD10COD"


def test_dict_to_counts():
    cfg = make_cfg()
    d = frames_to_dict(cfg)
    out = dict_to_counts(d).sort(["source_col", "source_term"])
    expected = pl.DataFrame(
        {
            "source_col": ["COH_ICD10COD", "COH_ICD10COD", "TR_TRNAME", "TR_TRNAME"],
            "source_term": ["C18", "C50", "DrugA", "DrugB"],
            "frequency": [1, 2, 2, 1],
        }
    )
    assert out.equals(expected, null_equal=True)


def test_sheet_key_casing():
    df = pl.DataFrame(
        {"SubjectId": [1], "X": ["a"]},
    )
    cfg = EcrfConfig(configs=[], data=[SheetData(key="coh", data=df)])
    d = frames_to_dict(cfg)
    assert "COH_X" in d


def test_nulls_are_dropped_from_counts():
    df = pl.DataFrame(
        {
            "SubjectId": [1, 2, 3],
            "X": ["a", None, "a"],
        },
    )
    cfg = EcrfConfig(configs=[], data=[SheetData(key="COH", data=df)])
    out = dict_to_counts(frames_to_dict(cfg))
    assert out.filter(pl.col("source_term").is_null()).is_empty()
    assert out.filter(pl.col("source_term") == "a")["frequency"].item() == 2


def test_filter_unique_effect():
    df = pl.DataFrame(
        {
            "SubjectId": [1, 1, 2],
            "X": ["a", "a", "a"],
        },
    )
    cfg = EcrfConfig(configs=[], data=[SheetData(key="COH", data=df)])
    d = frames_to_dict(cfg)
    out = dict_to_counts(d)
    # drop duplicate rows
    assert out.select(pl.col("frequency")).item() == 2, "uniqueness per row (not raw data or value-unique)"


def test_uuid_is_unique_scoped_to_source():
    df_1 = pl.DataFrame({"source_col": ["A", "A", "A"], "source_term": ["a", "a", "b"], "frequency": [2, 2, 1]})

    df_2 = pl.DataFrame({"source_col": ["C", "C", "D"], "source_term": ["a", "a", "s"], "frequency": [2, 2, 1]})

    out_1 = add_term_id(df_1, id_scope="per_scope")
    ids_1 = out_1.select(pl.col("term_id")).to_series()
    assert ids_1[0] == ids_1[1]
    assert ids_1[0] != ids_1[2]

    out_2 = add_term_id(df_2, id_scope="per_scope")
    ids_2 = out_2.select(pl.col("term_id")).to_series()
    assert ids_2[0] != ids_1[0]
    assert ids_2[0] != ids_2[2]


def test_uuid_is_unique_global():
    df_1 = pl.DataFrame({"source_col": ["A", "A", "A"], "source_term": ["a", "a", "b"], "frequency": [2, 2, 1]})

    df_2 = pl.DataFrame({"source_col": ["C", "C", "D"], "source_term": ["a", "a", "s"], "frequency": [2, 2, 1]})

    out_1 = add_term_id(df_1, id_scope="global")
    ids_1 = out_1.select(pl.col("term_id")).to_series()
    assert ids_1[0] == ids_1[1]
    assert ids_1[0] != ids_1[2]

    out_2 = add_term_id(df_2, id_scope="global")
    ids_2 = out_2.select(pl.col("term_id")).to_series()
    assert ids_2[0] == ids_1[0]
    assert ids_2[0] != ids_2[2]
