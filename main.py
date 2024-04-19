import asyncio
import json
from typing import List

import aio_pika
from aio_pika import ExchangeType
from strategies.elastic_more_like_this_similarity_strategy import MoreLikeThisSimilarityStrategy
from strategies.elastic_title_similarity_strategy import TitleSyntacticSimilarityStrategy

from commons.models import Entity, Reference, Contribution, Contributor, Result
from simple_duplicate_detector import SimpleDuplicateDetector
from strategies.notice_semantic_similarity_strategy import NoticeSemanticSimilarityStrategy
from strategies.title_semantic_similarity_strategy import TitleSemanticSimilarityStrategy

EXCHANGE_NAME = "publications"

QUEUE_NAME = "svp-scientific-repo"

QUEUE_TOPIC = "event.references.reference.*"

AMQP_PARAMS = "amqp://guest:guest@127.0.0.1/"

strategies = [
    NoticeSemanticSimilarityStrategy(),
    TitleSemanticSimilarityStrategy(),
    TitleSyntacticSimilarityStrategy(),
    MoreLikeThisSimilarityStrategy()
]


async def main() -> None:
    connection = await create_connection()
    async with connection:
        queue = await create_queue(connection)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    handle_message(message)


def handle_message(message: aio_pika.IncomingMessage):
    global strategies
    entity, reference = extract_information(message)
    print(reference.titles)
    if len(reference.contributions) == 0:
        reference.contributions = [
            Contribution(rank=0, role="Author",
                         contributor=Contributor(name=entity.name, source=entity.identifiers[0].type,
                                                 source_identifier=entity.identifiers[0].value, name_variants=[]))
        ]
    raw_candidates: List[Result] = []
    for strategy in strategies:
        strategy.load_reference(entity, reference)
        raw_candidates.extend(strategy.get_similar_references(entity, reference))
    raw_candidates = [candidate for candidate in raw_candidates if not SimpleDuplicateDetector(candidate.reference1, candidate.reference2).is_duplicate()]
    candidates = {}
    for candidate in raw_candidates:
        if candidate.reference2.unique_identifier() in candidates:
            candidates[
                candidate.reference2.unique_identifier()].similarity_strategies += candidate.similarity_strategies
            candidates[candidate.reference2.unique_identifier()].scores += candidate.scores
        else:
            candidates[candidate.reference2.unique_identifier()] = candidate

    for identifier, candidate in candidates.items():
        dict_ = {
            "text": f"{reference.html_comparaison_table(candidate.reference2, candidate.similarity_strategies, candidate.scores)}",
            "entity": entity.dict(),
            "reference_1": reference.dict(),
            "reference_2": candidate.reference2.dict(),
        }
        with open("data.jsonl", "a") as f:
            f.write(json.dumps(dict_, default=str) + "\n")


def extract_information(message) -> tuple[Entity, Reference]:
    string_to_parse = message.body.decode("utf-8")
    parsed_json = json.loads(string_to_parse)
    reference_json = parsed_json['reference_event']['reference']
    entity_json = parsed_json['entity']
    entity = Entity(**entity_json)
    reference = Reference(**reference_json)
    return entity, reference


async def create_connection():
    connection = await aio_pika.connect_robust(
        AMQP_PARAMS,
    )
    return connection


async def create_queue(connection):
    channel = await connection.channel()
    publication_exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.TOPIC,
    )
    await channel.set_qos(prefetch_count=10)
    queue = await channel.declare_queue(QUEUE_NAME, auto_delete=False, exclusive=False)
    await queue.bind(publication_exchange, QUEUE_TOPIC)
    return queue


if __name__ == '__main__':
    asyncio.run(main())
