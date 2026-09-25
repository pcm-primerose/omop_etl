from omop_etl.infra.io.path_planner import run_root
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.omop.core.io import OmopTableExporter
from omop_etl.omop.models.rows import PersonRow
from omop_etl.omop.models.tables import OmopTables


def _person(person_id: int, *, gender_source_value: str | None = None) -> PersonRow:
    return PersonRow(
        person_id=person_id,
        gender_concept_id=8507,
        year_of_birth=1980,
        race_concept_id=0,
        ethnicity_concept_id=0,
        person_source_value=f"p{person_id}",
        gender_source_concept_id=0,
        race_source_concept_id=0,
        ethnicity_source_concept_id=0,
        gender_source_value=gender_source_value,
    )


class TestOmopTableExporter:
    def test_writes_a_csv_per_populated_table_under_the_run_scoped_omop_dir(self, tmp_path):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, [_person(1)])

        base_out = tmp_path / "out"
        meta = RunMetadata.create("IMPRESS")

        written = OmopTableExporter(base_out).write(tables, meta)

        expected_path = run_root(base_out, meta) / "omop" / "PERSON.csv"
        assert written == {OmopTables.PERSON: expected_path}
        assert expected_path.exists()

    def test_skips_tables_with_no_rows(self, tmp_path):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, [_person(1)])
        # DRUG_ERA etc. never populated -- no CSV should appear for them

        written = OmopTableExporter(tmp_path).write(tables, RunMetadata.create("IMPRESS"))

        assert set(written) == {OmopTables.PERSON}

    def test_null_fields_are_written_as_bare_unquoted_empty_fields(self, tmp_path):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, [_person(1, gender_source_value=None)])

        written = OmopTableExporter(tmp_path).write(tables, RunMetadata.create("IMPRESS"))

        raw = written[OmopTables.PERSON].read_text()
        header, row = raw.splitlines()
        gender_source_value_index = header.split(",").index("gender_source_value")
        # bare/unquoted None, not "", copy w. format csv reads this as NULL
        assert row.split(",")[gender_source_value_index] == ""
        assert '""' not in row

    def test_writes_all_columns_for_a_row_with_no_nulls(self, tmp_path):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, [_person(1, gender_source_value="m")])

        written = OmopTableExporter(tmp_path).write(tables, RunMetadata.create("IMPRESS"))

        lines = written[OmopTables.PERSON].read_text().splitlines()
        header = lines[0].split(",")
        row = lines[1].split(",")
        assert row[header.index("person_id")] == "1"
        assert row[header.index("gender_source_value")] == "m"

    def test_no_populated_tables_writes_nothing(self, tmp_path):
        written = OmopTableExporter(tmp_path).write(OmopTables(), RunMetadata.create("IMPRESS"))

        assert written == {}
