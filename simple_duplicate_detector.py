import re
import unicodedata

from commons.models import Reference


# Assuming all necessary Pydantic models are defined above as given...

class SimpleDuplicateDetector:
    def __init__(self, reference1: Reference, reference2: Reference):
        self.reference1 = reference1
        self.reference2 = reference2

    def is_duplicate(self) -> bool:
        if self.compare_identifiers():
            return True
        if self.compare_book_identifiers():
            return True
        if self.compare_manifestations():
            return True
        if not self.compare_titles():
            return False
        if self.both_notices_have_abstract() and not self.compare_abstracts():
            return False
        if self.both_notices_have_document_types() and not self.compare_document_types():
            return False
        if not self.compare_contributors():
            return False
        return True

    @staticmethod
    def normalize_text(text: str) -> str:
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
        contributors1 = {self.normalize_text(contrib.contributor.name) for contrib in
                         self.reference1.contributions}
        contributors2 = {self.normalize_text(contrib.contributor.name) for contrib in
                         self.reference2.contributions}
        return contributors1 == contributors2

    def both_notices_have_abstract(self):
        return self.reference1.abstracts and self.reference2.abstracts

    def both_notices_have_document_types(self):
        return self.reference1.document_type and self.reference2.document_type

    def compare_identifiers(self) -> bool:
        identifiers1 = {(ident.type, ident.value) for ident in self.reference1.identifiers}
        identifiers2 = {(ident.type, ident.value) for ident in self.reference2.identifiers}
        # for all doi identifiers, remove the https://doi.org/ prefix through a dedicated method
        # and compare the two sets of identifiers
        identifiers1 = {self.remove_doi_prefix(ident) for ident in identifiers1}
        identifiers2 = {self.remove_doi_prefix(ident) for ident in identifiers2}
        # lowercase all identifiers
        identifiers1 = {(ident[0].lower(), ident[1].lower()) for ident in identifiers1}
        identifiers2 = {(ident[0].lower(), ident[1].lower()) for ident in identifiers2}
        return bool(identifiers1.intersection(identifiers2))

    def compare_book_identifiers(self) -> bool:
        if self.reference1.book and self.reference2.book:
            isbn_13_1 = self.reference1.book.isbn13
            isbn_13_2 = self.reference2.book.isbn13
            isbn_10_1 = self.reference1.book.isbn10
            isbn_10_2 = self.reference2.book.isbn10
            return self.same_isbn(isbn_13_1, isbn_13_2) or self.same_isbn(isbn_10_1, isbn_10_2)
        return False

    @staticmethod
    def same_isbn(isbn1: str, isbn2: str) -> bool:
        return isinstance(isbn1, str) and isinstance(isbn2, str) and isbn1.strip() == isbn2.strip()

    def compare_manifestations(self) -> bool:
        uris_1 = {manifestation.page for manifestation in self.reference1.manifestations}
        uris_2 = {manifestation.page for manifestation in self.reference2.manifestations}
        uri_identifiers_1 = {ident.value for ident in self.reference1.identifiers if
                             ident.type == 'uri'}
        uri_identifiers_2 = {ident.value for ident in self.reference2.identifiers if
                             ident.type == 'uri'}
        uris_1.update(uri_identifiers_1)
        uris_2.update(uri_identifiers_2)
        uris_1 = {self.remove_trailing_id(url) for url in uris_1}
        uris_2 = {self.remove_trailing_id(url) for url in uris_2}
        # lowercase all urls
        uris_1 = {url.lower() for url in uris_1}
        uris_2 = {url.lower() for url in uris_2}
        return bool(uris_1.intersection(uris_2))

    def remove_trailing_id(self, url: str) -> str:
        return re.sub(r'/id$', '', url)

    def remove_doi_prefix(self, identifier: tuple) -> tuple:
        if identifier[0] == 'doi':
            return ('doi', re.sub(r'^https?://doi.org/', '', identifier[1]))
        return identifier
