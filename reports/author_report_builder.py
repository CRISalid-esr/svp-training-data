from collections import defaultdict
from itertools import chain
from typing import List

from commons.models import Entity, Reference


class AuthorReportBuilder:
    def __init__(self, entity: Entity):
        self.visual_ids = None
        self.entity = entity
        self.references = {}
        self.trivial_duplicates = []
        self.potential_duplicates = []
        self.report_lines = None
        self.potential_duplicates_chains = defaultdict(list)

    def add_reference(self, reference: Reference):
        if reference.unique_identifier() not in self.references:
            self.references[reference.unique_identifier()] = reference

    def add_trivial_duplicate(self, reference1: Reference, reference2: Reference):
        self.trivial_duplicates.append((reference1.unique_identifier(), reference2.unique_identifier()))
        # Remote duplicated tuples from trivial_duplicates
        self.trivial_duplicates = list(set(self.trivial_duplicates))

    def get_trivial_duplicates(self):
        return self.trivial_duplicates

    def add_potential_duplicate(self, reference1: Reference, reference2: Reference):
        self.potential_duplicates.append((reference1.unique_identifier(), reference2.unique_identifier()))
        # Remote duplicated tuples from potential_duplicates
        self.potential_duplicates = list(set(self.potential_duplicates))

    def dump_report(self, directory: str):
        self.generate_report()
        with open(f"{directory}/{self.get_main_entity_id(self.entity)}.txt", "w") as f:
            f.write("\n".join(self.report_lines))

    def _group_trivial_duplicates(self):
        parent = {}

        def find(x):
            if parent[x] == x:
                return x
            else:
                parent[x] = find(parent[x])
                return parent[x]

        def union(x, y):
            root_x = find(x)
            root_y = find(y)
            if root_x != root_y:
                parent[root_y] = root_x

        # Initialize parent pointers
        all_ids = set(chain.from_iterable(self.trivial_duplicates))
        for i in all_ids:
            parent[i] = i

        # Union-Find to group duplicates
        for x, y in self.trivial_duplicates:
            union(x, y)

        # Group by root
        groups = defaultdict(list)
        for i in all_ids:
            root = find(i)
            groups[root].append(i)

        return list(groups.values())

    def generate_report(self):
        self.report_lines = []
        self.visual_ids = {}
        self._print_entity()
        single_references, trivial_groups = self._compute_groups()
        self._print_single_references(single_references)
        self._print_trivial_duplicates(trivial_groups)
        self._print_potential_duplicates()
        potential_duplicate_chains = self._build_potential_duplicate_chains()
        self._print_potential_duplicate_chains(potential_duplicate_chains)

    def _compute_groups(self):
        trivial_groups = self._group_trivial_duplicates()
        trivial_ids = set(chain.from_iterable(trivial_groups))
        single_references = [ref for ref_id, ref in self.references.items() if ref_id not in trivial_ids]
        return single_references, trivial_groups

    def _build_potential_duplicate_graph(self):
        graph = defaultdict(set)
        for a, b in self.potential_duplicates:
            graph[a].add(b)
            graph[b].add(a)
        return graph

    @staticmethod
    def _visit(node, graph, visited):
        stack = [node]
        component = []
        while stack:
            current = stack.pop()
            if current not in visited:
                visited.add(current)
                component.append(current)
                for neighbor in graph[current]:
                    if neighbor not in visited:
                        stack.append(neighbor)
        return component

    def _print_potential_duplicates(self):
        self.print_subtitle("Pairs of Potential Duplicates")

        chain_separator = " <---> "
        border_char = "*"
        border_length = 50

        already_printed = set()

        for i, (ref1, ref2) in enumerate(self.potential_duplicates):
            visual_ref1 = self.visual_ids[ref1]
            visual_ref2 = self.visual_ids[ref2]
            if (visual_ref1, visual_ref2) in already_printed or (visual_ref2, visual_ref1) in already_printed:
                continue
            already_printed.add((visual_ref1, visual_ref2))
            self.report_lines.append(f"\t{border_char * border_length}")
            self.report_lines.append(f"\tPair {i + 1}: {visual_ref1} <---> {visual_ref2}")
            self.report_lines.append(f"\tIdentifiers: {ref1} <---> {ref2}")
            self.report_lines.append(f"\t{border_char * border_length}")

    def _build_potential_duplicate_chains(self):
        potential_duplicates_graph = self._build_potential_duplicate_graph()
        visited = set()
        components = []
        for node in potential_duplicates_graph:
            if node not in visited:
                component = self._visit(node, potential_duplicates_graph, visited)
                components.append(sorted(component))
        return components

    def _print_potential_duplicate_chains(self, potential_duplicate_chains: List[List[str]]):
        self.print_subtitle("Chain of potential Duplicates")

        chain_separator = " <---> "
        border_char = "*"
        border_length = 50

        for i, chain in enumerate(potential_duplicate_chains):
            visual_chain = [self.visual_ids[ref_id] for ref_id in chain]
            visual_chain = list(dict.fromkeys(visual_chain))

            self.report_lines.append(f"\t{border_char * border_length}")
            self.report_lines.append(f"\tChain n°{i + 1} : {chain_separator.join(visual_chain)}")
            self.report_lines.append(f"\tIdentifiers: {chain_separator.join(chain)}")
            self.report_lines.append(f"\t{border_char * border_length}")

        self.report_lines.append("=" * border_length)

    def _print_trivial_duplicates(self, trivial_groups):
        self.print_subtitle("Trivial Duplicate Groups")
        group_number = 1
        for group in trivial_groups:
            self._print_group_header(group_number)
            reference_number = 1
            for ref_id in group:
                reference = self.references[ref_id]
                if reference:
                    visual_id = f"G{group_number}-R{reference_number}"
                    self._print_reference(reference, visual_id)
                    reference_number += 1
                    self.visual_ids[reference.unique_identifier()] = f"G{group_number}"
            group_number += 1

    def _print_single_references(self, lonely_references):
        self.print_subtitle("Not Duplicated References:")
        reference_number = 1
        for reference in lonely_references:
            visual_id = f"R{reference_number}"
            self._print_reference(reference, visual_id)
            reference_number += 1
            self.visual_ids[reference.unique_identifier()] = visual_id

    def _print_entity(self):
        author_info = self.entity.name
        author_identifiers = ", ".join([f"{identifier.type}: {identifier.value}" for identifier in
                                        self.entity.identifiers])
        self.report_lines.append(f"Author: {author_info}")
        self.report_lines.append(f"Identifiers: {author_identifiers}\n")

    def _print_group_header(self, group_number):
        self.report_lines.append(
            "─────────────────────────────────────────────────────────────────────────────\n"
            f"Group n°{group_number}\n"
            "─────────────────────────────────────────────────────────────────────────────\n"
        )

    def print_subtitle(self, subtitle):
        self.report_lines.append("=" * 50)
        self.report_lines.append(subtitle)
        self.report_lines.append("=" * 50)

    def _print_reference(self, reference, visual_id):
        unique_identifier = reference.unique_identifier()
        title = reference.titles[0].value if reference.titles else "No title available"
        authors = ', '.join([contrib.contributor.name for contrib in reference.contributions])
        document_type = ', '.join([doc_type.label for doc_type in reference.document_type])
        publication_date = reference.issued.strftime("%d-%m-%Y") if reference.issued else "No date available"
        journal_or_book_title = (reference.issue.journal.titles[0]
                                 if reference.issue and reference.issue.journal and reference.issue.journal.titles
                                 else "No journal/book title available")
        issue = reference.issue.volume if reference.issue and reference.issue.volume else "No issue available"
        subjects = ', '.join([subject.pref_labels[0].value for subject in reference.subjects if subject.pref_labels])

        # Function to truncate long strings and add ellipsis
        def truncate(text, max_length):
            return (text[:max_length - 3] + '...') if len(text) > max_length else text

        max_length = 150  # Define a reasonable max length for each field

        synthesis = (
            "\t────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n"
            f"\tReference n°{visual_id}\n"
            "\t────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n"
            "\tSource Identifier: {}\n"
            "\tTitle: {}\n"
            "\tAuthors: {}\n"
            "\tDocument Type: {}\n"
            "\tPublication Date: {}\n"
            "\tJournal/Book Title: {}\n"
            "\tIssue: {}\n"
            "\tSubjects: {}\n"
            "\t────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n"
        ).format(
            truncate(unique_identifier, max_length),
            truncate(title, max_length),
            truncate(authors, max_length),
            truncate(document_type, max_length),
            truncate(publication_date, max_length),
            truncate(journal_or_book_title, max_length),
            truncate(issue, max_length),
            truncate(subjects, max_length)
        )

        self.report_lines.append(synthesis)
        self.report_lines.append("")

    @classmethod
    def get_main_entity_id(cls, entity: Entity) -> str:
        main_id = None
        for identifier_type in ['idref', 'orcid', 'id_hal_s', 'id_hal_i', 'scopus_eid']:
            for identifier in entity.identifiers:
                if identifier.type == identifier_type:
                    main_id = identifier
                    break
            if main_id:
                break
        if not main_id:
            raise ValueError(f"Entity without known identifier submitted for report: {entity}")
        return main_id.value
