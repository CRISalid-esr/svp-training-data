import re
import unicodedata

from commons.models import Reference


# Assuming all necessary Pydantic models are defined above as given...

class ExclusionFilter:
    def __init__(self, reference: Reference):
        self.reference = reference

    def discard(self) -> bool:
        if self.is_hal_or_idref_book_from_scanr():
            return True
        return False

    def is_hal_or_idref_book_from_scanr(self) -> bool:
        if not str.lower(self.reference.harvester) == "scanr":
            return False
        target_strings = ["book", "chapter"]
        is_book = any(any(target in str.lower(doctype.label) for target in target_strings) for doctype in self.reference.document_type)
        if not is_book:
            return False
        identifier_types = {identifier.type for identifier in self.reference.identifiers}
        identifier_types_to_exclude = ["hal"]
        if any(identifier_type in identifier_types_to_exclude for identifier_type in identifier_types):
            return True
        url_segments =["sudoc", "idref"]
        if any(identifier.type == "uri" and any(segment in identifier.value for segment in url_segments) for identifier in self.reference.identifiers):
            return True
        source_identifier_prefixes = ['sudoc']
        if any(self.reference.source_identifier.startswith(prefix) for prefix in source_identifier_prefixes):
            return True
        return False
