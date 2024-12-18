from typing import Generator

from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores.elasticsearch import ElasticsearchStore

from commons.models import Entity, Reference, Result
from strategies.similarity_strategy import SimilarityStrategy

ES_PASSWORD = "elastic"

ES_USER = "elastic"

ES_INDEX = "notices_semantic_minilml12v2_1"

ES_URL = "http://localhost:9200"


class NoticeSemanticSimilarityStrategy(SimilarityStrategy):
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
        summary = self._build_summary(entity, reference)
        metadata = reference.dict() | {"id": identifier}
        self.elastic_vector_search.add_texts([summary],
                                             ids=[identifier],
                                             metadatas=[
                                                 metadata
                                             ])

    def _build_summary(self, entity, reference):
        titles = " | ".join(
            [title.value for title in reference.titles])
        subtitles = " | ".join(
            [subtitle.value for subtitle in reference.subtitles])
        abstracts = " | ".join(
            [abstract.value for abstract in reference.abstracts])
        document_types = " | ".join(
            list(set([document_type.label for document_type in
                      reference.document_type])))
        if len(reference.contributions) > 0:
            contributors = " | ".join(
                [contribution.contributor.name for contribution in
                 reference.contributions])
        else:
            contributors = entity.name
        summary = f"{document_types}\n{titles}\n{subtitles}\{contributors}\{abstracts}"
        return summary

    def get_similar_references(self, entity: dict, reference: dict) -> Generator[Result, None, None]:
        identifier = reference.unique_identifier()
        summary = self._build_summary(entity, reference)
        search_results = self.elastic_vector_search.similarity_search_with_score(summary, k=20)
        filtered_results = [document for document in search_results if document[1] > self.SIMILARITY_THRESHOLD
                            and document[1] < 1
                            and not document[0].metadata['id'] == identifier]
        converted_results = [(Reference(**document[0].metadata), document[1]) for document in filtered_results]
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
        return f"Similarité sémantique des notices min : {self.SIMILARITY_THRESHOLD} "
