from typing import Generator

from commons.models import Entity, Reference, Result
from strategies.elastic_basic_similarity_strategy import ElasticBasicSimilarityStrategy

SCORE_THRESHOLD = 200  # Score set at 20 begin to give good results. The most precise documents are at 30+


class TitleSyntacticSimilarityStrategy(ElasticBasicSimilarityStrategy):
    ES_INDEX = "elastic_basic_similarity"

    def __init__(self):
        super().__init__()

    def get_similar_references(
            self, entity: Entity, reference: Reference
    ) -> Generator[Result, None, None]:
        """
        Get similar references from the elastic search index
        """
        identifier = reference.unique_identifier()
        source_identifier = reference.source_identifier
        title_values = [title.value for title in reference.titles]
        query_results = []

        for title in title_values:
            title_length = len(title)

            # First, analyze the text using the custom analyzer
            analyzed_response = self.es.indices.analyze(
                index=self.ES_INDEX,
                body={
                    "analyzer": "custom_analyzer",
                    "text": title
                }
            )

            query = {
                "query": {
                    "fuzzy": {
                        "titles.value.normalized": {
                            "value": analyzed_response['tokens'][0]['token'],
                            #max edit distance allowed by elastic search
                            "fuzziness": 2,
                            "prefix_length": 0,
                            "max_expansions": 10000
                        }
                    }
                }
            }

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
        for result in deduplicated_results:
            yield Result(reference1=reference,
                         reference2=Reference(**{**result["_source"]}),
                         scores=[result["_score"]],
                         similarity_strategies=[self.get_name()]
                         )

    def get_name(self) -> str:
        return f"Similarité syntaxique des titres"
