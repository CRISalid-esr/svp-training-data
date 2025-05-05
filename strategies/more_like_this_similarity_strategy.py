from typing import Generator

from commons.models import Entity, Reference, Result
from strategies.synctactic_similarity_strategy import SyntacticSimilarityStrategy

SCORE_THRESHOLD = 180


class MoreLikeThisSimilarityStrategy(SyntacticSimilarityStrategy):
    ES_INDEX = "mld_syntactic_1"

    def get_similar_references(
            self, entity: Entity, reference: Reference
    ) -> Generator[Result, None, None]:
        if not self.initialization_success:
            return
        fields = ["titles.value", "abstracts.value", "contributors.contributor.name", "subjects.pref_labels.value"]
        query = {
            "query": {
                "more_like_this": {
                    "fields": fields,
                    "like": [{"_index": self.ES_INDEX, "_id": reference.unique_identifier()}],
                    "min_term_freq": 1,
                    "max_query_terms": 12,
                }
            }
        }

        query_results = self.es.search(index=self.ES_INDEX, body=query)
        for result in query_results["hits"]["hits"]:
            if result["_score"] >= SCORE_THRESHOLD:
                yield Result(
                    reference1=reference,
                    reference2=Reference(**result["_source"]),
                    scores=[result["_score"]],
                    similarity_strategies=[self.get_name()]
                )

    def get_name(self) -> str:
        return f"Similarité syntaxique des notices : {SCORE_THRESHOLD} "
