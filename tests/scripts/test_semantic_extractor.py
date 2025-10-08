import polars as pl

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
