from elasticsearch import Elasticsearch

from commons.models import Entity, Reference
from strategies.similarity_strategy import SimilarityStrategy

ES_PASSWORD = "elastic"
ES_USER = "elastic"
ES_INDEX = "test_elastic_fuzziness_similarity"
ES_URL = "http://localhost:9200"

ES_INDEX_MAPPING = {

        "properties": {
            "abstracts": {
                "properties": {
                    "language": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "contributions": {
                "properties": {
                    "contributor": {
                        "properties": {
                            "name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "source": {
                                "type": "keyword",
                            },
                            "source_identifier": {
                                "type": "keyword",
                            }
                        }
                    },
                    "rank": {
                        "type": "long"
                    },
                    "role": {
                        "type": "keyword",
                    }
                }
            },
            "document_type": {
                "properties": {
                    "label": {
                        "type": "keyword",
                    },
                    "uri": {
                        "type": "keyword",
                    }
                }
            },
            "harvester": {
                "type": "keyword",
            },
            "id": {
                "type": "keyword",
            },
            "identifiers": {
                "properties": {
                    "type": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "keyword",
                    }
                }
            },
            "source_identifier": {
                "type": "keyword",
            },
            "subjects": {
                "properties": {
                    "alt_labels": {
                        "properties": {
                            "language": {
                                "type": "keyword",
                            },
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }
                    },
                    "pref_labels": {
                        "properties": {
                            "language": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }
                    },
                    "uri": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "subtitles": {
                "properties": {
                    "language": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                },

            },
            "titles": {
                "properties": {
                    "language": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            }
        }

}


class ElasticTitleFuzzinessSimilarityStrategy(SimilarityStrategy):
    def __init__(self):
        self.es = Elasticsearch(
            [ES_URL],
            http_auth=(ES_USER, ES_PASSWORD),
            verify_certs=True,
        )
        if not self.es.indices.exists(index=ES_INDEX):
            self.es.indices.create(index=ES_INDEX, mappings=ES_INDEX_MAPPING)

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
        query = {
            "query": {
                "bool": {
                    "must": [
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

                    "must_not": {
                        "term": {
                            "_id": identifier
                        }
                    }
                }
            }
        }

        min_score = 15
        query_results = self.es.search(index=ES_INDEX, body=query, min_score=min_score)
        returned_results = [Reference(**{**result['_source'], 'score': result['_score']}) for result in query_results['hits']['hits']]
        return self._add_common_informations(returned_results)

    def get_name(self) -> str:
        return "Elastic title fuzziness similarity"
