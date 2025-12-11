from dataclasses import dataclass

# todo notes:
# 1. take in harmonized and mapped data
# 2. build athena CDM to DB container (or package with repo containing this vocab as csv files, and query this?)
# 3. make db connection code (takes container from podman network)
# 4. structurally map (modelling the CDM tables?)
# 5. connect to db container, load all data in correct order
from omop_etl.harmonization.datamodels import HarmonizedData, Patient


# also need preparation stage:
#   - should either load OMOP CDM in database container with Athena vocab or package with csv files of Athena
#   - data needs to be mapped to concepts not in semantic mapping file (query athena or store in semantic mapper)
#       - or no, since these concepts are fields of my datamodel, they are invariant, and map to the same concept regardless of data in the field
#   - need to determinstically create UID ints for ID fields for tables
# then need loader stage:
#   - what prepared data (semantically mapped or not) goes to what excat omop tables/fields
#   - load preparaed data into postgres container by port, in correct order

# todo: tirsdag
#  [ ] get data into minimal OMOP CDM locally:
#  [ ] just need to build person, observation_period and cmd_source
#  [ ] set this up locally with ETL container + DB container
#       - try with podman
#       - when this auotmation works, iterate to add more fields
#       - don't let tests drift behind

# todo:
#   [ ] make static mapping csv
#       [ ] go through all fields in Patient and map static fields
#           - start with just required ones for minimal cdm
#   [ ] package and load to struct
#       - can make configurable later if needed, using StaticFieldConfig pattern from semantic search
#   [ ] model omop tables
#   [ ] mapper takes Patient, does semantic + structural mapping, returns table row structs
#   [ ] loader takes tavble rows and loads to DB container

# [ ] need to handle non-mapped fields like non-target lesion size, nadir size, nadir size at baseline, etc
#     put these in measurement and make them queryable by source field name (handle in row creation)


@dataclass
class StructuralField:
    pass


class StructuralMapper:
    def __init__(self, harmonized_data: HarmonizedData):
        self.harmonized_data = harmonized_data

    def map(self):
        pass


# todo:
#   patient has:
#   - person_id (autogen int, needs to be deterministic & idempotent)
#   - gender_concept_id
#   - year_of_birth
#   - race & ethnicity thing (says required, but probably not?)
#   should create some struct, since need to pass foreign keys around to diff table constructors (e.g. person_id)

# todo:
#   observation_pediod has:
#   - observation_period_id (int for each record per patient, i only have one, but make it work for several)
#   - person_id (person id, autogen int id from patient)
#   - observation_period_start_date (first high qual date)
#   - observation_period_end_date (last high qual date, or cdm date (?))
#   - period_type_concept_id (concept id for CRF: https://athena.ohdsi.org/search-terms/terms?domain=Type+Concept&standardConcept=Standard&page=1&pageSize=15&query=)
#   just keep one period record for all patients (inclusion date or first data not in historical data, end date or last recorded date)

# todo:
#   cdm_source has:
#   - cdm_source_name (name of cdm instance)
#   - cdm_source_abbreviation (abbreviation of cdm instance name)
#   - cdm_holder (holder of cdm instance)
#   - source_release_date (date data was extracted, just use date of run as this is too hard to determine)
#   - cdm_release_date (should be tied to ETL versions, for now just use date of run)
#   - cdm_version_concept_id (concept id representing cdm version)
#   - vocabulary_version (cdm version)


def map_fields(patient: Patient):
    if patient.sex is not None:
        pass

    pass
