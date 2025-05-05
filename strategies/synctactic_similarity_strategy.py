from elasticsearch import Elasticsearch

from commons.es_params import ESParams
from commons.models import Entity, Reference
from strategies.similarity_strategy import SimilarityStrategy


class SyntacticSimilarityStrategy(SimilarityStrategy):

    ES_INDEX_SETTINGS = {
        "analysis": {
            "analyzer": {
                "custom_analyzer": {
                    "tokenizer": "keyword",
                    "filter": ["lowercase", "asciifolding", "remove_punctuation", "trim"]
                }
            },
            "filter": {
                "remove_punctuation": {
                    "type": "pattern_replace",
                    "pattern": "[^\\p{L}\\p{Nd}]+",
                    "replacement": ""
                }
            }
        }
    }

    ES_INDEX_MAPPING = {
        "properties": {
            "abstracts": {
                "properties": {
                    "language": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            },
            "contributions": {
                "properties": {
                    "contributor": {
                        "properties": {
                            "name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                            "last_name": {
                                "type": "text",
                                "fields": {
                                    "normalized": {
                                        "type": "text",
                                        "store": "true",
                                        "analyzer": "custom_analyzer",
                                        "search_analyzer": "custom_analyzer"
                                    }
                                },
                            },
                            "source": {
                                "type": "keyword",
                            },
                            "source_identifier": {
                                "type": "keyword",
                            },
                        }
                    },
                    "rank": {"type": "long"},
                    "role": {
                        "type": "keyword",
                    },
                }
            },
            "document_type": {
                "properties": {
                    "label": {
                        "type": "keyword",
                    },
                    "uri": {
                        "type": "keyword",
                    },
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
                    },
                }
            },
            "manifestations": {
                "properties": {
                    "additional_files": {
                        "type": "keyword",
                    },
                    "download_url": {
                        "type": "keyword",
                    },
                    "page": {
                        "type": "keyword",
                    },
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
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                        }
                    },
                    "pref_labels": {
                        "properties": {
                            "language": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                        }
                    },
                    "uri": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            },
            "subtitles": {
                "properties": {
                    "language": {
                        "type": "keyword",
                    },
                    "value": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
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
                            "keyword": {"type": "keyword", "ignore_above": 256},
                            "normalized": {
                                "type": "text",
                                "analyzer": "custom_analyzer",
                                "search_analyzer": "custom_analyzer"
                            }
                        },
                    }

                }
            },
        }
    }

    def __init__(self):
        self.initialization_success = False
        params = ESParams()
        try:
            self.es = Elasticsearch(
                [params.url],
                http_auth=(params.user, params.password),
                verify_certs=False,
            )
            if not self.es.indices.exists(index=self.ES_INDEX):
                self.es.indices.create(index=self.ES_INDEX, mappings=self.ES_INDEX_MAPPING,
                                       settings=self.ES_INDEX_SETTINGS)

            self.initialization_success = True
        except Exception as e:
            print(f"Error connecting to ES: {e}")
            # display connexion parameters for debugging
            print(f"ES URL: {params.url}")
            print(f"ES User: {params.user}")
            print(f"ES Password: {params.password}")

    def load_reference(self, entity: Entity, reference: Reference):
        """
        Add the reference to the elastic search index
        """
        if not self.initialization_success:
            return
        identifier = reference.unique_identifier()
        reference.compute_last_names()
        metadata = reference.dict() | {"id": identifier}
        self.es.index(index=self.ES_INDEX, id=identifier, body=metadata)
