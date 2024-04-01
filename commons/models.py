from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class ReferenceIdentifier(BaseModel):
    type: str
    value: str


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
    issn: Optional[str] = None
    eissn: Optional[str] = None
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


class Reference(BaseModel):
    source_identifier: str
    harvester: str
    identifiers: List[ReferenceIdentifier]
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
    score: Optional[float] = None

    def unique_identifier(self) -> str:
        return f"{self.harvester}-{self.source_identifier}"

    def html_comparaison_table(self, other_reference: 'Reference') -> str:
        table_html = "<table class=\"duplicate-comparaison\">\n"
        table_html += "    <tr>\n"
        table_html += "        <th>Field</th>\n"
        table_html += f"        <th>Reference n°1 <span class=\"badge\">{self.harvester}<br/>{self.source_identifier}</span></th>\n"
        table_html += f"        <th>Reference n°2 <span class=\"badge\">{other_reference.harvester}<br/>{other_reference.source_identifier}</span></th>\n"
        table_html += "    </tr>\n"

        # Add rows for each aspect
        fields = [
            ("Title(s)", [title.value for title in self.titles], [title.value for title in other_reference.titles]),
            ("Subtitle(s)", [subtitle.value for subtitle in self.subtitles],
             [subtitle.value for subtitle in other_reference.subtitles]),
            ("Abstract(s)", [abstract.value for abstract in self.abstracts],
             [abstract.value for abstract in other_reference.abstracts]),
            ("Subjects", [", ".join(subject.pref_labels[0].value for subject in self.subjects)],
             [", ".join(
                 subject.pref_labels[0].value if subject.pref_labels else "" for subject in other_reference.subjects)]),
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
                f"{self.issue.journal.titles[0]} ({self.issue.journal.issn or 'no issn'})"] if self.issue and self.issue.journal else [],
             [
                 f"{other_reference.issue.journal.titles[0]} ({other_reference.issue.journal.issn or 'no issn'})"] if other_reference.issue and other_reference.issue.journal else []),
            ("Volume", [self.issue.volume] if self.issue and self.issue.volume else [],
             [other_reference.issue.volume] if other_reference.issue and other_reference.issue.volume else []),
            ("Number", self.issue.number if self.issue and self.issue.number else [],
             other_reference.issue.number if other_reference.issue and other_reference.issue.number else []),
            ("Pages", [self.pages] if self.pages else [], [other_reference.pages] if other_reference.pages else []),

        ]

        for aspect, values1, values2 in fields:
            table_html += "    <tr>\n"
            table_html += f"        <td>{aspect}</td>\n"
            table_html += "        <td>{}</td>\n".format("</br>".join(values1))
            table_html += "        <td>{}</td>\n".format("</br>".join(values2))
            table_html += "    </tr>\n"

        # Add similarity strategies row
        if other_reference.similarity_strategies:
            table_html += f"    <tr>\n"
            table_html += f"        <td colspan=\"3\" style=\"text-align: center;\">Similarity Strategies: {', '.join(other_reference.similarity_strategies)}</td>\n"
            table_html += "    </tr>\n"

        table_html += "</table>\n"

        return table_html


class EntityIdentifier(BaseModel):
    type: str
    value: str


class Entity(BaseModel):
    identifiers: List[EntityIdentifier]
    name: str
