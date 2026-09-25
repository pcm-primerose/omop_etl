-- Modified from the stock OHDSI CDM 5.5 PostgreSQL DDL
-- (OHDSI/CommonDataModel, inst/ddl/5.5/postgresql/).
-- Only change from default: `@cdmDatabaseSchema` to `public`.
--postgresql CDM Primary Key Constraints for OMOP Common Data Model 5.5
ALTER TABLE public.person  ADD CONSTRAINT xpk_person PRIMARY KEY (person_id);
ALTER TABLE public.observation_period  ADD CONSTRAINT xpk_observation_period PRIMARY KEY (observation_period_id);
ALTER TABLE public.visit_occurrence  ADD CONSTRAINT xpk_visit_occurrence PRIMARY KEY (visit_occurrence_id);
ALTER TABLE public.visit_detail  ADD CONSTRAINT xpk_visit_detail PRIMARY KEY (visit_detail_id);
ALTER TABLE public.condition_occurrence  ADD CONSTRAINT xpk_condition_occurrence PRIMARY KEY (condition_occurrence_id);
ALTER TABLE public.drug_exposure  ADD CONSTRAINT xpk_drug_exposure PRIMARY KEY (drug_exposure_id);
ALTER TABLE public.procedure_occurrence  ADD CONSTRAINT xpk_procedure_occurrence PRIMARY KEY (procedure_occurrence_id);
ALTER TABLE public.device_exposure  ADD CONSTRAINT xpk_device_exposure PRIMARY KEY (device_exposure_id);
ALTER TABLE public.measurement  ADD CONSTRAINT xpk_measurement PRIMARY KEY (measurement_id);
ALTER TABLE public.observation  ADD CONSTRAINT xpk_observation PRIMARY KEY (observation_id);
ALTER TABLE public.note  ADD CONSTRAINT xpk_note PRIMARY KEY (note_id);
ALTER TABLE public.note_nlp  ADD CONSTRAINT xpk_note_nlp PRIMARY KEY (note_nlp_id);
ALTER TABLE public.specimen  ADD CONSTRAINT xpk_specimen PRIMARY KEY (specimen_id);
ALTER TABLE public.location  ADD CONSTRAINT xpk_location PRIMARY KEY (location_id);
ALTER TABLE public.care_site  ADD CONSTRAINT xpk_care_site PRIMARY KEY (care_site_id);
ALTER TABLE public.provider  ADD CONSTRAINT xpk_provider PRIMARY KEY (provider_id);
ALTER TABLE public.payer_plan_period  ADD CONSTRAINT xpk_payer_plan_period PRIMARY KEY (payer_plan_period_id);
ALTER TABLE public.cost  ADD CONSTRAINT xpk_cost PRIMARY KEY (cost_id);
ALTER TABLE public.drug_era  ADD CONSTRAINT xpk_drug_era PRIMARY KEY (drug_era_id);
ALTER TABLE public.dose_era  ADD CONSTRAINT xpk_dose_era PRIMARY KEY (dose_era_id);
ALTER TABLE public.condition_era  ADD CONSTRAINT xpk_condition_era PRIMARY KEY (condition_era_id);
ALTER TABLE public.episode  ADD CONSTRAINT xpk_episode PRIMARY KEY (episode_id);
ALTER TABLE public.metadata  ADD CONSTRAINT xpk_metadata PRIMARY KEY (metadata_id);