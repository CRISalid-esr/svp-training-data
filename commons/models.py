from datetime import datetime
from typing import List, Optional

from nameparser import HumanName
from pydantic import BaseModel


class ReferenceIdentifier(BaseModel):
    type: str
    value: str


class ReferenceManifestation(BaseModel):
    page: Optional[str]


class Title(BaseModel):
    value: str
    language: Optional[str]


class Subtitle(BaseModel):
    value: str
    language: Optional[str]


class DocumentType(BaseModel):
    uri: str
    label: str


class Abstract(BaseModel):
    value: str
    language: Optional[str]


class Journal(BaseModel):
    issn: List[str] = []
    eissn: List[str] = []
    publisher: Optional[str] = None
    titles: List[str] = []


class Issue(BaseModel):
    volume: Optional[str] = None
    number: List[str] = []
    rights: Optional[str] = None
    date: Optional[datetime] = None
    journal: Journal


class Contributor(BaseModel):
    source: str
    source_identifier: Optional[str]
    name: str
    last_name: Optional[str] = None
    name_variants: List[str]


class Contribution(BaseModel):
    rank: Optional[int]
    contributor: Contributor
    role: str


class PrefLabel(BaseModel):
    value: str
    language: Optional[str]


class AltLabel(BaseModel):
    value: str
    language: Optional[str]


class Concept(BaseModel):
    uri: Optional[str]
    pref_labels: List[PrefLabel]
    alt_labels: List[AltLabel]

class Book(BaseModel):
    title: str | None = None
    title_variants: list[str] = []
    isbn10: str | None = None
    isbn13: str | None = None
    publisher: str | None = None


class Reference(BaseModel):
    source_identifier: str
    harvester: str
    identifiers: List[ReferenceIdentifier]
    manifestations: Optional[List[ReferenceManifestation]] = []
    titles: List[Title]
    subtitles: List[Subtitle]
    abstracts: List[Abstract]
    subjects: List[Concept]
    document_type: List[DocumentType]
    contributions: List[Contribution]
    similarity_strategies: List[str] = []
    issued: Optional[datetime] = None
    created: Optional[datetime] = None
    issue: Optional[Issue] = None
    pages: Optional[str] = None
    book: Optional[Book] = None

    def compute_last_names(self)-> None:
        #use HumanName to populate the last_name field of each contributor
        for contribution in self.contributions:
            if contribution.contributor.name:
                contribution.contributor.last_name = HumanName(contribution.contributor.name).last

    def unique_identifier(self) -> str:
        return f"{self.harvester}-{self.source_identifier}"

    def html_comparaison_table(self, other_reference: 'Reference', strategies, scores) -> str:
        table_html = "<table class=\"duplicate-comparaison\">\n"
        table_html += "    <tr>\n"
        table_html += "        <th>Field</th>\n"
        table_html += f"        <th>Reference n°1 <span class=\"badge\">{self.harvester}<br/>{self.source_identifier}</span></th>\n"
        table_html += f"        <th>Reference n°2 <span class=\"badge\">{other_reference.harvester}<br/>{other_reference.source_identifier}</span></th>\n"
        table_html += "    </tr>\n"

        table_html += "    <tr>\n"
        table_html += "        <td>Identifiers</td>\n"
        table_html += "        <td>{}</td>\n".format("</br>".join(
            [f"{identifier.type}: {identifier.value}" for identifier in (self.identifiers or [])]))
        table_html += "        <td>{}</td>\n".format("</br>".join(
            [f"{identifier.type}: {identifier.value}" for identifier in
             (other_reference.identifiers or [])]))
        table_html += "    </tr>\n"

        # Add ISBN section if applicable
        def get_isbn_html(reference):
            if not reference.book:
                return ""
            isbn_parts = []
            if hasattr(reference.book, "isbn10") and reference.book.isbn10:
                isbn_parts.append(f"ISBN-10: {reference.book.isbn10}")
            if hasattr(reference.book, "isbn13") and reference.book.isbn13:
                isbn_parts.append(f"ISBN-13: {reference.book.isbn13}")
            return "</br>".join(isbn_parts)

        table_html += "    <tr>\n"
        table_html += "        <td>ISBN</td>\n"
        table_html += f"        <td>{get_isbn_html(self)}</td>\n"
        table_html += f"        <td>{get_isbn_html(other_reference)}</td>\n"
        table_html += "    </tr>\n"

        # Add other fields
        fields = [
            ("Title(s)", [title.value for title in self.titles],
             [title.value for title in other_reference.titles]),
            ("Subtitle(s)", [subtitle.value for subtitle in self.subtitles],
             [subtitle.value for subtitle in other_reference.subtitles]),
            ("Abstract(s)", [abstract.value for abstract in self.abstracts],
             [abstract.value for abstract in other_reference.abstracts]),
            ("Subjects", [", ".join(subject.pref_labels[0].value for subject in self.subjects if
                                    len(subject.pref_labels) > 0)],
             [", ".join(subject.pref_labels[0].value if subject.pref_labels else "" for subject in
                        other_reference.subjects)]),
            ("Document Type(s)", list(set([doc_type.label for doc_type in self.document_type])),
             list(set([doc_type.label for doc_type in other_reference.document_type]))),
            ("Contributions",
             [
                 f"{contribution.contributor.name or contribution.contributor.source_identifier}, role: {contribution.role or 'Unknown'}"
                 for contribution in self.contributions],
             [
                 f"{contribution.contributor.name or contribution.contributor.source_identifier}, role: {contribution.role or 'Unknown'}"
                 for contribution in
                 other_reference.contributions]),
            ("Origin", [f"{self.harvester} / {self.source_identifier}"],
             [f"{other_reference.harvester} / {other_reference.source_identifier}"]),
            ("Publication Date", [self.issued.strftime("%d-%m-%Y") if self.issued else ""],
             [other_reference.issued.strftime("%d-%m-%Y") if other_reference.issued else ""]),
            ("Creation Date", [self.created.strftime("%d-%m-%Y") if self.created else ""],
             [other_reference.created.strftime("%d-%m-%Y") if other_reference.created else ""]),
            ("Journal", [
                f"{self.issue.journal.titles[0] if self.issue.journal.titles else 'no title'} ({', '.join(self.issue.journal.issn) if self.issue.journal.issn else 'no issn'})"] if self.issue and self.issue.journal else [],
             [
                 f"{other_reference.issue.journal.titles[0] if other_reference.issue.journal.titles else 'no title'} ({', '.join(other_reference.issue.journal.issn) if other_reference.issue.journal.issn else 'no issn'})"] if other_reference.issue and other_reference.issue.journal else []),
            ("Volume", [self.issue.volume] if self.issue and self.issue.volume else [],
             [
                 other_reference.issue.volume] if other_reference.issue and other_reference.issue.volume else []),
            ("Number", self.issue.number if self.issue and self.issue.number else [],
             other_reference.issue.number if other_reference.issue and other_reference.issue.number else []),
            ("Pages", [self.pages] if self.pages else [],
             [other_reference.pages] if other_reference.pages else []),
            ("Manifestations",
             [f"{manifestation.page}" for manifestation in (self.manifestations or [])],
             [f"{manifestation.page}" for manifestation in (other_reference.manifestations or [])]),
        ]

        for aspect, values1, values2 in fields:
            table_html += "    <tr>\n"
            table_html += f"        <td>{aspect}</td>\n"
            table_html += "        <td>{}</td>\n".format("</br>".join(values1))
            table_html += "        <td>{}</td>\n".format("</br>".join(values2))
            table_html += "    </tr>\n"

        # Add similarity strategies row
        if strategies:
            table_html += f"    <tr>\n"
            table_html += f"        <td colspan=\"3\" style=\"text-align: center;\">Similarity Strategies : {', '.join([f'{strat} ({score})' for strat, score in zip(strategies, scores)])}</td>\n"
            table_html += "    </tr>\n"

        table_html += "</table>\n"

        return table_html


class EntityIdentifier(BaseModel):
    type: str
    value: str


class Entity(BaseModel):
    identifiers: List[EntityIdentifier]
    name: str


class Result(BaseModel):
    reference1: Reference
    reference2: Reference
    similarity_strategies: List[str]
    scores: List[float]
