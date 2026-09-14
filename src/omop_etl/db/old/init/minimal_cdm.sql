-- db/init/minimal_cdm.sql
-- https://github.com/Smart-PACER-Registry/omopv5_4_setup/blob/main/CommonDataModel-5.4.0/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_ddl.sql

CREATE TABLE IF NOT EXISTS person (
  person_id                 bigint        NOT NULL,
  gender_concept_id         integer       NOT NULL,
  year_of_birth             integer       NOT NULL,
  month_of_birth            integer       NULL,
  day_of_birth              integer       NULL,
  birth_datetime            timestamp     NULL,
  race_concept_id           integer       NOT NULL,
  ethnicity_concept_id      integer       NOT NULL,
  person_source_value       varchar(50)   NULL,
  gender_source_value       varchar(50)   NULL,
  gender_source_concept_id  integer       NULL,
  race_source_value         varchar(50)   NULL,
  race_source_concept_id    integer       NULL,
  ethnicity_source_value    varchar(50)   NULL,
  ethnicity_source_concept_id integer     NULL,
  CONSTRAINT xpk_person PRIMARY KEY (person_id)
);

CREATE TABLE IF NOT EXISTS observation_period (
  observation_period_id         bigint NOT NULL,
  person_id                     bigint NOT NULL,
  observation_period_start_date date   NOT NULL,
  observation_period_end_date   date   NOT NULL,
  period_type_concept_id        integer NOT NULL,
  CONSTRAINT xpk_observation_period PRIMARY KEY (observation_period_id)
);

CREATE TABLE IF NOT EXISTS cdm_source (
  cdm_source_name               varchar(255) NOT NULL,
  cdm_source_abbreviation       varchar(25)  NOT NULL,
  cdm_holder                    varchar(255) NOT NULL,
  source_description            TEXT NULL,
  source_documentation_reference varchar(255) NULL,
  cdm_etl_reference             varchar(255) NULL,
  source_release_date           date         NOT NULL,
  cdm_release_date              date         NOT NULL,
  cdm_version                   varchar(10)  NULL,
  cdm_version_concept_id        integer      NOT NULL,
  vocabulary_version            varchar(20)  NOT NULL
);
