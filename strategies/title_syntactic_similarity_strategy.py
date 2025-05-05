from typing import Generator

from commons.models import Entity, Reference, Result
from strategies.common_titles import common_titles
from strategies.synctactic_similarity_strategy import SyntacticSimilarityStrategy


class TitleSyntacticSimilarityStrategy(SyntacticSimilarityStrategy):
    ES_INDEX = "title_syntactic_1"
    LEVENSHTEIN_THRESHOLD = 2
    MEANINGLESS_TITLES = []
    MIN_MEANINGFUL_TITLE_LENGTH = 12

    def __init__(self):
        super().__init__()
        # if the composite approach (with author names) has been used
        self.composite = False

    def get_similar_references(
            self, entity: Entity, reference: Reference
    ) -> Generator[Result, None, None]:
        """
        Get similar references from the elastic search index
        """
        if not self.initialization_success:
            return
        identifier = reference.unique_identifier()
        reference.compute_last_names()
        source_identifier = reference.source_identifier
        title_values = [title.value for title in reference.titles]
        query_results = []

        for title in title_values:
            title_length = len(title)

            analyze_response = self.es.indices.analyze(
                index=self.ES_INDEX,
                body={
                    "analyzer": "custom_analyzer",
                    "text": title
                }
            )
            analyzed_title = analyze_response['tokens'][0]['token']
            if self._title_is_meaning_less(title, analyzed_title):
                composite = True
                query = self.title_authors_query(analyzed_title, reference.contributions)
            else:
                query = self.title_only_query(analyzed_title)

        raw_result = self.es.search(index=self.ES_INDEX, body=query)
        raw_results = raw_result["hits"]["hits"]
        # eclude : ScanR : halhalshs-00511995,	HAL : halshs-00511995
        # exclude all results where source identifier  is contained in the reference source identifier
        raw_results = [result for result in raw_results if
                       source_identifier not in result["_source"]["source_identifier"]]
        # exclude all results where source identifier contains reference source identifier
        raw_results = [result for result in raw_results if
                       result["_source"]["source_identifier"] not in source_identifier]
        # exclude all results where unique identifier is the same as the reference unique identifier
        raw_results = [result for result in raw_results if result["_id"] != identifier]
        query_results.append(raw_results)

        # flatten the list of lists
        query_results = [item for sublist in query_results for item in sublist]
        deduplicated_results_hash = {}
        for result in query_results:
            if result["_id"] in deduplicated_results_hash:
                if result["_score"] > deduplicated_results_hash[result["_id"]]["_score"]:
                    deduplicated_results_hash[result["_id"]] = result
            else:
                deduplicated_results_hash[result["_id"]] = result
        deduplicated_results = deduplicated_results_hash.values()
        # if both document titles and reference titles are in common_titles, the similarity is not relevant : filter the document out
        for result in deduplicated_results:
            reference2 = Reference(**{**result["_source"]})
            # concatenate all string titles from ref1 and ref2
            str_titles = [title.value for title in reference.titles] + [title.value for title in reference2.titles]
            if common_titles(str_titles):
                continue
            yield Result(reference1=reference,
                         reference2=reference2,
                         scores=[result["_score"]],
                         similarity_strategies=[self.get_name()]
                         )

    def _title_is_meaning_less(self, title, analyzed_title):
        """
        title has only one word,
        or analyzed_title is shorter than MIN_MEANINGFUL_TITLE_LENGTH
        or analyzed_title is in a list of meaningless titles
        """
        return len(title.split()) == 1 or len(
            analyzed_title) < self.MIN_MEANINGFUL_TITLE_LENGTH or analyzed_title in self.MEANINGLESS_TITLES

    def title_only_query(self, analyzed_title):
        query = {
            "query": self.fuzzy_title_query_block(analyzed_title)
        }
        return query

    def fuzzy_title_query_block(self, analyzed_title):
        return {
            "fuzzy": {
                "titles.value.normalized": {
                    "value": analyzed_title,
                    # max edit distance allowed by elastic search
                    "fuzziness": self.LEVENSHTEIN_THRESHOLD,
                    "prefix_length": 0,
                    "max_expansions": 10000
                }
            }
        }

    def title_authors_query(self, analyzed_title, contributions):
        authors_match_block = [
            {"match": {"contributions.contributor.last_name.normalized": contribution.contributor.last_name}} for
            contribution in contributions]
        query = {
            "query": {
                "bool": {
                    "must": [
                        self.fuzzy_title_query_block(analyzed_title),
                        {
                            "bool": {
                                "should": authors_match_block,
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            }
        }

        return query

    def get_name(self) -> str:
        return "Similarité syntaxique des titres et des auteurs" \
            if self.composite \
            else "Similarité syntaxique des titres"
