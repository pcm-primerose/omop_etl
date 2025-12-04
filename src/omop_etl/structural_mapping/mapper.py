# todo:
# 1. take in harmonized data and semantic mapped data
# 2. build athena
# 3. connect to db and query with polars or sql
# 4. do i model the cdm and hardcode fields --> tables/fields in the CDM?
#   - need to figure out what's best to do here, set up simple example
#   - and think of how this'll fit in with a deployed app
#   - so need a separarate container with Athena/OMOP CDM
#     or, package with Athena CDM and do internal queries here,
#     and then build all data in the ETL, do single load pass to DB container ... ?
#   - so yeah: should set up network in ETL? or at least, definately, connect to DB container
#     just mimic the TSD setup exactly with separate db container and etl container
#     but running the containers is outside of source, so just dep inject connection or how?
#     etl container is the ETL code itself so that can be ignored,
#     so we can just pass db conn as dep injection

# 1. take in harmonized and mapped data
# 2. build athena CDM to DB container (or package with vocab and query this?)
# 3. make db connection code (takes container from podman network)
# 4. structurally map (modelling the CDM tables?)
# 5. connect to db container, load all data in correct order

