from typing import Generator

from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores.elasticsearch import ElasticsearchStore

from commons.models import Entity, Reference, Result
from strategies.common_titles import common_titles
from strategies.similarity_strategy import SimilarityStrategy

ES_PASSWORD = "elastic"
ES_USER = "elastic"
ES_INDEX = "titles_semantic_minilml12v2_1"
ES_URL = "http://localhost:9200"


class TitleSemanticSimilarityStrategy(SimilarityStrategy):
    SIMILARITY_THRESHOLD = 0.96

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

    def get_similar_references(self, entity: dict, reference: dict) -> Generator[Result, None, None]:
        identifier = reference.unique_identifier()
        titles = " | ".join([title.value for title in reference.titles])
        search_results = self.elastic_vector_search.similarity_search_with_score(titles, k=20)
        filtered_results = [(document, score) for document, score in search_results
                            if self.SIMILARITY_THRESHOLD < score < 1.0
                            and not document.metadata['id'] == identifier]
        # if both document titles and reference titles are in common_titles, the similarity is not relevant : filter the document out
        # extract string titles from filtered result and from references
        str_titles = [t['value'] for t in [result[0].metadata['titles'][0] for result in filtered_results]] + \
                     [t.value for t in [title for title in reference.titles]]
        filtered_results = [(document, score) for document, score in filtered_results if
                            not common_titles(str_titles)]
        converted_results = [(Reference(**document.metadata), score) for document, score in filtered_results]
        deduplicated_results = [result for result in converted_results if
                                not self._identifiers_from_same_source(reference, result[0])
                                and not self._reference_with_common_identifier(reference, result[0])]
        for result in deduplicated_results:
            yield Result(
                reference1=reference,
                reference2=result[0],
                scores=[result[1]],
                similarity_strategies=[self.get_name()]
            )

    def get_name(self) -> str:
        return f"Similarité sémantique des titres min : {self.SIMILARITY_THRESHOLD} "
