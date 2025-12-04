# todo notes:
# 1. take in harmonized and mapped data
# 2. build athena CDM to DB container (or package with repo containing this vocab as csv files, and query this?)
# 3. make db connection code (takes container from podman network)
# 4. structurally map (modelling the CDM tables?)
# 5. connect to db container, load all data in correct order

# also need preparation stage:
#   - should either load OMOP CDM in database container with Athena vocab or package with csv files of Athena
#   - data needs to be mapped to concepts not in semantic mapping file (query athena or store in semantic mapper)
#       - or no, since these concepts are fields of my datamodel, they are invariant, and map to the same concept regardless of data in the field
#   - need to determinstically create UID ints for ID fields for tables
# then need loader stage:
#   - what prepared data (semantically mapped or not) goes to what excat omop tables/fields
#   - load preparaed data into postgres container by port, in correct order
