# notes:
# - ConceptMappingService is only thing exposed to row builders:
#     containing index of static/structural data and BatchQueryResults
# - make structural loader
# - make sure return structs align for diff loaders
# - row builder doesn't know anything about semantic query layer, csv, input formats etc
# - structural and static mapping doesn't need anything else, just dict lookups

# todo:
# [ ] clean up module structure
# [ ] emit row structure (AllRows:PersonRows)
# [ ] load this into DB
# [ ] make DB layer, set up with Podman and containers (mimic TSD)


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

