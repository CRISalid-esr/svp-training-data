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
    score: Optional[float] = None

    def unique_identifier(self) -> str:
        return f"{self.harvester}-{self.source_identifier}"

    def pretty_representation(self) -> str:
        title_str = ", ".join(title.value for title in self.titles)
        subtitle_str = ", ".join(subtitle.value for subtitle in self.subtitles)
        abstract_str = ", ".join(abstract.value for abstract in self.abstracts)
        subject_str = ", ".join(subject.pref_labels[0].value for subject in self.subjects)
        doc_type_str = ", ".join(list(set(doc_type.label for doc_type in self.document_type)))
        contribution_str = " - ".join(
            f"{contribution.contributor.name or contribution.contributor.source_identifier}, role: {contribution.role or 'Unknown'}"
            for contribution in self.contributions
        )
        similarity_strategies_str = ", ".join(self.similarity_strategies)

        representation = (
            f"Title(s): {title_str}\n"
            f"Subtitle(s): {subtitle_str}\n"
            f"Abstract(s): {abstract_str}\n"
            f"Subjects: {subject_str}\n"
            f"Document Type(s): {doc_type_str}\n"
            f"Contributions:\n{contribution_str}\n"
            f"Origin : {self.harvester} / {self.source_identifier}"
        )
        if similarity_strategies_str:
            representation += f"\nSimilarity Strategies: {similarity_strategies_str}"
        return representation

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
             [f"{other_reference.harvester} / {other_reference.source_identifier}"])
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
