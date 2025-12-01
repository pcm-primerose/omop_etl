# import sqlalchemy as sa


# 1. build local dev omop cdm w. postgresql
# 2. figure out minimal seetup requires (i.e. non-optional tables)
# 3. then probably (?) model entire schema
# 4. just start with setting up a minimal cdm with Person, Observation_period and Cdm_source tables
# 5. and use local podman with ETL docker image and psql docker image, test loading locally

# todo: set up minimal local cdm with podman

# Person:
#    required fields:
#       - person_id, gender_concept_id, year_of_birth (try and leave these as None, if not set to 0: race_concept_id, ethicity_concept_id)
#    also need to convert from str patient id to int person_id deterministically,
#    mayeb do this early in pipeline? in any case, need same data to generate same id, so we can update db instance
#    without creating entirely new cdm? also think about if there should be some provenance

# Observation_period:
#   required fields:
#       - observation_period_id, person_id, observation_period_start_date, observation_period_end_date, pediod_type_concept_id
#   - need to generate observation period id int for each observation period (what exactly does that mean? start of study to end, or each treatment, or what?)
#   - each observation entry needs start and end date
#   - provenance for what type of observation period, I guess e.g. treatment cycles, concomitant medications? what qualifies as an observational period
#     see: https://athena.ohdsi.org/search-terms/terms?domain=Type+Concept&standardConcept=Standard&page=1&pageSize=15&query=
#     so all are just ECR and we take inclusion date or start of treatment, to death or last treatment (?)

# Cdm_soure:
#   required fields:
#       - cdm_source_name, csm_source_abbreviation, cdm_holder, source_release_date, cdm_release_date, cdm_version_concept_id, vocabulary_version
#   self-explanatory

# so usually people load all of Athena into the CDM, which populates the Vocabulary tables, and can then be used to query
# that nice i guess bc then we dont need to a-priori match evertytong but on the other hand it's bad for TSD,
# each time we do anything I'd need to -

# def process_age(data):
#     # do the actual sql logic for merging, harmonization etc
#     # make modular are not stateless (other than mb session)
#     pass

# Maybe something like this?
#   - would need to model each omop CDM table
#   wihch might be bad, if it updates or the schema changes?
#   idk if i can dynamically do this, but could just build the cdm
#   and duck-type input data to the cdm fields,
#   can then wrap datamodel in context objets, allowing dynamic loading of data of what is there to the correct field,
#   without having to explicitly structure the mapped semantic objects to CDM (boundary)
#   validated, harmonized data is one boundary, this can be mapped to semantic objects that don't need info about the database, then passed to the db loader?
#   or it might be easier to structure it as one object (harmonized data, semantic concepts, db info)?


# Current layer: Harmonized data models
# class TumorType:  # Raw harmonized data
#     icd10_code: str
#     main_tumor_type: str
#     ...
#
#
# # New layer: OMOP-mapped models
# @dataclass
# class OmopCondition:
#     condition_concept_id: int  # Standardized OMOP concept
#     condition_source_value: str  # Original value
#     condition_source_concept_id: Optional[int]  # ICD10 mapping
#     person_id: int
#     condition_start_date: datetime
#     ...
#
#
# # Transformer/Mapper classes
# class TumorTypeToOmopMapper:
#     def __init__(self, concept_mapper: ConceptMapper):
#         self.concept_mapper = concept_mapper
#
#     def map(self, tumor_type: TumorType, patient: Patient) -> List[OmopCondition]:
#         # Semantic mapping logic here
#         concept_id = self.concept_mapper.find_standard_concept(
#             source_code=tumor_type.icd10_code,
#             source_vocabulary='ICD10',
#             domain='Condition'
#         )
#
#         return [OmopCondition(
#             condition_concept_id=concept_id,
#             condition_source_value=tumor_type.main_tumor_type,
#             ...
#         )]


# import sqlalchemy as sa
# from ..sql.sql_logic import process_age

# class ProcessPerson:
#     def __init__(self, source_data, output_data) -> None:
#         self.source_data = source_data
#         self.output_data = output_data
#
#     def run(self) -> None:
#         # run private methods (if any) to process person
#         # or just call sql methods directly
#         # log and use sa session
#         pass

# or just have mapped data in dataclass and load this into a omop dataclass then dump as dict then bulk load to postgres:


# One dataclass per OMOP table:
# @dataclass
# class ConditionOccurrence:  # Maps to condition_occurrence table
#     condition_occurrence_id: int
#     person_id: int
#     condition_concept_id: int
#     condition_start_date: date
#     condition_start_datetime: Optional[datetime]
#     condition_source_value: Optional[str]
#     condition_source_concept_id: Optional[int]
#
#     # ... all other OMOP fields
#
#     class Meta:
#         table_name = "condition_occurrence"
#         primary_key = "condition_occurrence_id"

# @dataclass
# class OmopTransformationResult:
#     """Container for all OMOP tables data"""
#     persons: List[Person] = field(default_factory=list)
#     condition_occurrences: List[ConditionOccurrence] = field(default_factory=list)
#     drug_exposures: List[DrugExposure] = field(default_factory=list)
#     observations: List[Observation] = field(default_factory=list)
#     measurements: List[Measurement] = field(default_factory=list)
#
#     # ... other OMOP tables
#
#     def add_condition(self, condition: ConditionOccurrence):
#         self.condition_occurrences.append(condition)
#
#     def get_all_records(self) -> Dict[str, List]:
#         """Returns all records grouped by table name"""
#         return {
#             "person": self.persons,
#             "condition_occurrence": self.condition_occurrences,
#             "drug_exposure": self.drug_exposures,
#             # ...
#         }

# class PatientToOmopMapper:
#     def __init__(self, concept_mapper: ConceptMapper):
#         self.concept_mapper = concept_mapper
#         self.person_id_counter = 1
#         self.condition_id_counter = 1
#
#     def map_patient(self, patient: Patient) -> OmopTransformationResult:
#         result = OmopTransformationResult()
#
#         # Map to Person table
#         person = Person(
#             person_id=self.person_id_counter,
#             gender_concept_id=self._map_gender(patient.sex),
#             year_of_birth=self._calculate_birth_year(patient.age),
#             # ...
#         )
#         result.persons.append(person)
#
#         # Map tumor to Condition Occurrence
#         if patient.tumor_type:
#             condition = self._map_tumor_to_condition(
#                 patient.tumor_type,
#                 person.person_id
#             )
#             result.condition_occurrences.append(condition)
#
#         # Map drugs to Drug Exposure
#         if patient.study_drugs:
#             for drug_exposure in self._map_drugs(patient.study_drugs, person.person_id):
#                 result.drug_exposures.append(drug_exposure)
#
#         self.person_id_counter += 1
#         return result

# class OmopDatabaseLoader:
#     def __init__(self, connection):
#         self.connection = connection
#
#     def load(self, omop_data: OmopTransformationResult):
#         """Load all OMOP data into database"""
#         for table_name, records in omop_data.get_all_records().items():
#             if records:
#                 self._bulk_insert(table_name, records)
#
#     def _bulk_insert(self, table_name: str, records: List[Any]):
#         """Generic bulk insert for any OMOP table"""
#         if not records:
#             return
#
#         # Convert dataclasses to dicts
#         data = [asdict(record) for record in records]
#
#         # Use pandas or your preferred method
#         df = pd.DataFrame(data)
#         df.to_sql(table_name, self.connection, if_exists='append', index=False)

