from typing import Any, Optional
import re
from src.harmonization.parsing.coercion import TypeCoercion
from src.harmonization.parsing.core import CoreParsers

# todo: layer 3 (consider keeping in one class for now)
#   acts on domain-specific data (in specific data model)


class TumorTypeParsers:
    """Domain-specific parsers for tumor type data data"""

    @staticmethod
    def parse_icd10_code(value: Any) -> Optional[str]:
        """Parse and validate ICD-10 code format"""
        # coerce (or make this two calls in implementation?)
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        normalized = str_value.upper()
        # validate ICD-10 format: Letter + 2-3 digits + optional decimal + digits
        if not re.match(r"^[A-Z]\d{2,3}(\.\d+)?$", normalized):
            raise ValueError(
                f"Invalid ICD-10 code format: {value}, expected format like C30 or B38.79"
            )

        return normalized
