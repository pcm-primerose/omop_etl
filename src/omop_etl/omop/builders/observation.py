# cdm spec

# so this table should contain all observations that are not populating:
# measurement, drug exposure, condition, procedure occurrence, device occurrence, condition occurrence etc.
# they *cannot* be of domain: Condition, Procedure, Drug, Specimen, Measurement or Device.

# so from the patient data this leaves what domain classes / patient scalars, and what fields and domains?
# think optimally do this table last, easier that way.

# todo: implement condition and device builders first:
# anything that lands in condition, procedure, drug, measurement or device can't go in observation

# anyways, can use:
# cohort_name,
# evaluable_for_efficacy_analysis
# has_any_adverse_events
# number_of_adverse_events
# number_of_serious_adverse_events
# has_clinical_benefit_at_week_16
# end_of_treatment_reason
# end_of_treatment_date
# lost_to_followup (bool, date)
# best_overall_response (but need non-measurement concepts)

# maybe:
# AE outcome, AE was serious, AE related to treatment status, AE turned serious date,
# tumor assessment date of progression, nadir, etc?
# study drugs?

"""
ObservationRow:
    required fields:
    observation_id: unique ID for each entry
    person_id: person id from service
    observation_concept_id: semantic mapped concept id
        There is no specified domain that the Concepts in this table must adhere to.
        The only rule is that records with Concepts in the Condition, Procedure, Drug,
        Measurement, or Device domains MUST go to the corresponding table.
        - so we grab
    observation_date: date, required
    observation_type_concept_id:

    optional fields:
    observation_datetime: dt.datetime | None = None
    value_as_number: float | None = None
    value_as_string: Annotated[str | None, pd_field(max_length=60)] = None
    value_as_concept_id: int | None = None
    qualifier_concept_id: int | None = None
    unit_concept_id: int | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    observation_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    observation_source_concept_id: int | None = None
    unit_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    qualifier_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    value_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    observation_event_id: int | None = None
    obs_event_field_concept_id: int | None = None
"""
