from abc import ABC, abstractmethod
from functools import wraps

from commons.models import Entity, Reference


class SimilarityStrategy(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def load_reference(self, entity: Entity, reference: Reference):
        pass

    @abstractmethod
    def get_similar_references(self, entity: Entity, reference: Reference) -> list[Reference]:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass

    def _identifiers_from_same_source(self, reference1: str, reference2: Reference) -> bool:
        """
        Exemple, source identifier of reference1 is 'hal-hal-02954829' and reference2 has "hal-02954829" as source identifier
        or reference1 has "hal-02954829" as source identifier and reference2 has "hal-hal-02954829" as source identifier

        :param reference1: Reference
        :param reference2: Reference
        :return: bool
        """

        source1 = reference1.source_identifier
        source2 = reference2.source_identifier
        return len(source1) > 0 and len(source2) > 0 and (source1 in source2 or source2 in source1)

    def _reference_with_common_identifier(self, reference1: str, reference2: Reference) -> bool:
        """
        Exemple, reference1 as an identifier of type 'doi' and value abcd" and reference2 has an identifier of type 'doi' and value "abcd"

        :param reference1:
        :param reference2:
        :return:
        """
        identical = any(
            [identifier1 == identifier2
             for identifier1 in reference1.identifiers
             for identifier2 in reference2.identifiers]
        )
        return identical

    def _add_common_informations(self, results):
        for result in results:
            result.similarity_strategies = [self.get_name()]
        return results
