from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores.elasticsearch import ElasticsearchStore

from commons.models import Entity, Reference
from strategies.similarity_strategy import SimilarityStrategy

ES_PASSWORD = "elastic"
ES_USER = "elastic"
ES_INDEX = "test_ref_vector_index_minilml12v2_titles"
ES_URL = "http://localhost:9200"


class TitleSemanticSimilarityStrategy(SimilarityStrategy):
    SIMILARITY_THRESHOLD = 0.95

    def __init__(self):
        self.embeddings = HuggingFaceEmbeddings(model_name="paraphrase-multilingual-MiniLM-L12-v2")
        self.elastic_vector_search = ElasticsearchStore(
            es_url=ES_URL,
            index_name=ES_INDEX,
            embedding=self.embeddings,
            es_user=ES_USER,
            es_password=ES_PASSWORD
        )

    def load_reference(self, entity: Entity, reference: Reference):
        identifier = reference.unique_identifier()
        titles = " | ".join([title.value for title in reference.titles])
        metadata = reference.dict() | {"id": identifier}
        self.elastic_vector_search.add_texts([titles], ids=[identifier], metadatas=[metadata])

    def get_similar_references(self, entity: dict, reference: dict) -> list[Reference]:
        identifier = reference.unique_identifier()
        titles = " | ".join([title.value for title in reference.titles])
        search_results = self.elastic_vector_search.similarity_search_with_score(titles, k=20)
        filtered_results = [document for document in search_results if document[1] > self.SIMILARITY_THRESHOLD
                            and document[1] < 1
                            and not document[0].metadata['id'] == identifier]
        converted_results = [Reference(**document[0].metadata) for document in filtered_results]
        deduplicated_results = [result for result in converted_results if
                                not self._identifiers_from_same_source(reference, result)
                                and not self._reference_with_common_identifier(reference, result)]
        return self._add_common_informations(deduplicated_results)

    def get_name(self) -> str:
        return f"Proximité sémantique des titres (seuil : {self.SIMILARITY_THRESHOLD})"
