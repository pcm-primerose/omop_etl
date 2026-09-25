
ALTER TABLE public.concept  ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
ALTER TABLE public.vocabulary  ADD CONSTRAINT unq_vocabulary UNIQUE (vocabulary_id);
ALTER TABLE public.domain  ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
ALTER TABLE public.concept_class  ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
ALTER TABLE public.relationship  ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);