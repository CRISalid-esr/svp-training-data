import re
import unicodedata

from commons.models import Reference


# Assuming all necessary Pydantic models are defined above as given...

class SimpleDuplicateDetector:
    def __init__(self, reference1: Reference, reference2: Reference):
        self.reference1 = reference1
        self.reference2 = reference2

    def is_duplicate(self) -> bool:
        if not self.compare_titles():
            return False
        if not self.compare_abstracts():
            return False
        if not self.compare_document_types():
            return False
        if not self.compare_contributors():
            return False
        return True

    def normalize_text(self, text: str) -> str:
        # Convert to normalized form, removing accents
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

        # Lowercase, strip leading/trailing spaces, and remove punctuation
        text = re.sub(r'[^\w\s]', '', text.lower()).strip()

        # Replace multiple spaces with a single space
        text = re.sub(' +', ' ', text)

        return text

    def compare_titles(self) -> bool:
        titles1 = {self.normalize_text(title.value) for title in self.reference1.titles}
        titles2 = {self.normalize_text(title.value) for title in self.reference2.titles}
        return titles1 == titles2

    def compare_abstracts(self) -> bool:
        abstracts1 = {self.normalize_text(abstract.value) for abstract in self.reference1.abstracts}
        abstracts2 = {self.normalize_text(abstract.value) for abstract in self.reference2.abstracts}
        return abstracts1 == abstracts2

    def compare_document_types(self) -> bool:
        doc_types1 = {doc_type.label for doc_type in self.reference1.document_type}
        doc_types2 = {doc_type.label for doc_type in self.reference2.document_type}
        # return true if there is a common element in both sets
        return bool(doc_types1.intersection(doc_types2))

    def compare_contributors(self) -> bool:
        contributors1 = {self.normalize_text(contrib.contributor.name) for contrib in self.reference1.contributions}
        contributors2 = {self.normalize_text(contrib.contributor.name) for contrib in self.reference2.contributions}
        return contributors1 == contributors2
