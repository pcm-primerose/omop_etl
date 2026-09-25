
/**************************
Standardized vocabularies
***************************/
CREATE INDEX idx_concept_concept_id  ON public.concept  (concept_id ASC);
CLUSTER public.concept  USING idx_concept_concept_id ;
CREATE INDEX idx_concept_code ON public.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabulary_id ON public.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON public.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON public.concept (concept_class_id ASC);
CREATE INDEX idx_vocabulary_vocabulary_id  ON public.vocabulary  (vocabulary_id ASC);
CLUSTER public.vocabulary  USING idx_vocabulary_vocabulary_id ;
CREATE INDEX idx_domain_domain_id  ON public.domain  (domain_id ASC);
CLUSTER public.domain  USING idx_domain_domain_id ;
CREATE INDEX idx_concept_class_class_id  ON public.concept_class  (concept_class_id ASC);
CLUSTER public.concept_class  USING idx_concept_class_class_id ;
CREATE INDEX idx_concept_relationship_id_1  ON public.concept_relationship  (concept_id_1 ASC);
CLUSTER public.concept_relationship  USING idx_concept_relationship_id_1 ;
CREATE INDEX idx_concept_relationship_id_2 ON public.concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON public.concept_relationship (relationship_id ASC);
CREATE INDEX idx_relationship_rel_id  ON public.relationship  (relationship_id ASC);
CLUSTER public.relationship  USING idx_relationship_rel_id ;
CREATE INDEX idx_concept_synonym_id  ON public.concept_synonym  (concept_id ASC);
CLUSTER public.concept_synonym  USING idx_concept_synonym_id ;
CREATE INDEX idx_concept_ancestor_id_1  ON public.concept_ancestor  (ancestor_concept_id ASC);
CLUSTER public.concept_ancestor  USING idx_concept_ancestor_id_1 ;
CREATE INDEX idx_concept_ancestor_id_2 ON public.concept_ancestor (descendant_concept_id ASC);
CREATE INDEX idx_drug_strength_id_1  ON public.drug_strength  (drug_concept_id ASC);
CLUSTER public.drug_strength  USING idx_drug_strength_id_1 ;
CREATE INDEX idx_drug_strength_id_2 ON public.drug_strength (ingredient_concept_id ASC);