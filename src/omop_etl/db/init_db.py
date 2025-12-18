# todo:
#   [ ] make basic setup with psql or sqlalchemy
#   [ ] load a table and read it out
#   [ ] populate with remaining tables
#   [ ] set up psql instance in docker, connect with podman, connect to existing or make new etc idk yet
#   [ ] load data to it idk how to mock tsd setup yet


# todo:
#   probably need to crate stateless functions that recieve connections, rows etc to populate?
#   [ ] make plan for setup, choose between psqpl and sqlalchemy (etc), what to do first, ...


# notes:
# jus dep inject connection and db meta from main and main get's it from run script (later config)
# can i pass connection cursor and db connection as args outside of context manager in psycopg?
# if so: write stateless core funcs that insert into tables
# or no just keep them pure and call from inside connection? that setup will be like a main entrypoint


# class OmopWriter:
#     def write(self, tables: OmopTables) -> None: ...
#
#
# class PostgresOmopWriter(OmopWriter):
#     def __init__(self, db: DbConfig):
#         self._db = db
