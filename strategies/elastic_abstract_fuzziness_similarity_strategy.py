from elasticsearch import Elasticsearch

from commons.models import Entity, Reference
from strategies.similarity_strategy import SimilarityStrategy

ES_PASSWORD = "elastic"
ES_USER = "elastic"
ES_INDEX = "test_elastic_fuzziness_similarity"
ES_URL = "http://localhost:9200"


class ElasticAbstractFuzzinessSimilarityStrategy(SimilarityStrategy):
    def __init__(self):
        self.es = Elasticsearch(
            [ES_URL],
            http_auth=(ES_USER, ES_PASSWORD),
            verify_certs=True,
        )
        if not self.es.indices.exists(index=ES_INDEX):
            self.es.indices.create(index=ES_INDEX)

    def load_reference(self, entity: Entity, reference: Reference):
        """
        Add the reference to the elastic search index
        """
        identifier = reference.unique_identifier()
        metadata = reference.dict() | {"id": identifier}
        self.es.index(index=ES_INDEX, id=identifier, body=metadata)

    def get_similar_references(self, entity: Entity, reference: Reference) -> list[Reference]:
        """
        Get similar references from the elastic search index
        """
        identifier = reference.unique_identifier()
        title_values = [title.value for title in reference.titles]
        abstract_values = [abstract.value for abstract in reference.abstracts]
        contributor_names = [contribution.contributor.name for contribution in
                             reference.contributions]

        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query": title_value,
                                            "fields": ["titles.value"],
                                            "fuzziness": "AUTO"
                                        }
                                    } for title_value in title_values
                                ]
                            }
                        }
                    ],
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query": abstract_value,
                                            "fields": ["abstract.value"],
                                            "fuzziness": "AUTO"
                                        }
                                    } for abstract_value in abstract_values
                                ]
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query": contributor_name,
                                            "fields": ["contributions.contributor.name"],
                                            "fuzziness": "AUTO"
                                        }
                                    } for contributor_name in contributor_names
                                ]
                            }
                        }
                    ],
                    "must_not": {
                        "term": {
                            "_id": identifier
                        }
                    }
                }
            }
        }
        query_results = self.es.search(index=ES_INDEX, body=query)
        returned_results = [Reference(**result['_source']) for result in query_results['hits']['hits']]
        return self._add_common_informations(returned_results)

    def get_name(self) -> str:
        return "Elastic abstract fuzziness similarity"
