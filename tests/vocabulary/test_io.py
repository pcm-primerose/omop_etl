from omop_etl.infra.io.path_planner import run_root
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.vocabulary.core.io import VocabularyExporter
from omop_etl.vocabulary.core.subset import scan_concept_subset
from tests.vocabulary.conftest import ConceptRow, write_concept_csv


class TestWriteConceptSubset:
    def test_writes_the_subset_under_the_run_scoped_vocabulary_dir(self, tmp_path):
        athena_dir = tmp_path / "athena"
        athena_dir.mkdir()
        write_concept_csv(athena_dir, ConceptRow(4112853), ConceptRow(8507))
        subset = scan_concept_subset(athena_dir, {4112853, 8507})

        base_out = tmp_path / "out"
        meta = RunMetadata.create("IMPRESS")
        exporter = VocabularyExporter(base_out=base_out)

        out_path = exporter.write_concept_subset(subset, meta)

        assert out_path == run_root(base_out, meta) / "vocabulary" / "concept_subset.tsv"
        written_ids = {int(line.split("\t")[0]) for line in out_path.read_text().splitlines()[1:]}
        assert written_ids == {4112853, 8507}
