from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import BuildResult
from omop_etl.omop.models.rows import EpisodeRow


# this is an abstraction over clinical tables (visit, drug, condition, procedure)
# so it has no new data, except for the concepts

# treatment episodes
# here we can populate episode_object_concept_id for each study drug, like e.g. this concept:

# 35804821	15595	Vismodegib monotherapy	Regimen	Standard	Valid	Regimen	HemOnc

# for the patient's study drug: Erivedge (Vismodegib)
# or alternatively, just use the TreatmentCycleComponent.source_name (which is the same as the study drug)
# if we find no mapping, we can leave this as id=0

# so for the study drugs, we have:
# Erivedge (Vismodegib)
# Tecentriq (Atezolizumab)
# Zelboraf (Vermurafenib)
# Cotellic (Cobimetinibhemifumarat)
# Phesgo (Pertuzumab and Trastuzumab)
# Lynparza (Olaparib)
# Piqray (Alpelisib) -- no concept for *just* Piqray (Alpelisib) (only for combination *with* Fulvestrant)
# Tafinlar (Dabrafenib)
# Mekinist (Trametinib)
# Avastin (Bevacizumab)
# Alecensa (Alectinib hydrochloride)
# Jemperli (Dostalimab)
# Pemazyre (Pemigatinib)
# Pemzyre
# Teva (imatinib)
# Fulvestrant
# Tepmetkop (Tepotinib)
# Pemigatinib
# Rozlytrek (Entrektinib)
# Bortezomib
# Melfalan
# Ceritinib
# Actinomycine D (maps by synonym: Dactinomycin)
# Zejula (Niraparib)
# Rybrevant (Amivantamab)
#


# note on Piqray (Alpalesib)
# so this is actually a combination therapy? in which case, I should map to two ingredients to drug_exposure?
# and create two instances of TreatmentCycleComponent? No because from the source data this is not *parseable* to two separate components,
# without a-priori knowledge (special rule for just this), so idk.
# in the ecrf, patients get Piqray as a study drug (without any Fulvestrant), or is Fulvestrant *included* in the Piqray treatment?
# some have fulvestrant as drug 1 and piqray as drug 2, most have just piqray (meaning it's probably monotherapy and not implicit combination)

# so i think i can map every study drug to the appropriate Regimen concept,
# jsut need to add that to the semantic mapping file

#


class EpisodeBuilder(OmopBuilder[EpisodeRow]):
    def build(self, ctx: BuildContext) -> BuildResult[EpisodeRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        pass
