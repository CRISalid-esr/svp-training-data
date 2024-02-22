import ast
import asyncio
import json
from typing import List

import aio_pika
from aio_pika import ExchangeType

from commons.models import Entity, Reference, Contribution, Contributor
from strategies.notice_semantic_similarity_strategy import NoticeSemanticSimilarityStrategy
from strategies.title_semantic_similarity_strategy import TitleSemanticSimilarityStrategy

EXCHANGE_NAME = "publications"

QUEUE_NAME = "svp-scientific-repo"

QUEUE_TOPIC = "event.references.reference.*"

AMQP_PARAMS = "amqp://guest:guest@127.0.0.1/"

strategies = [
    NoticeSemanticSimilarityStrategy(),
    TitleSemanticSimilarityStrategy(),
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
    raw_candidates: List[Reference] = []
    for strategy in strategies:
        strategy.load_reference(entity, reference)
        raw_candidates.extend(strategy.get_similar_references(entity, reference))
    # agregate candidates with same identifier but sum values of the "similarity_strategies" field
    candidates = {}
    for candidate in raw_candidates:
        if candidate.unique_identifier() in candidates:
            candidates[candidate.unique_identifier()].similarity_strategies += candidate.similarity_strategies
        else:
            candidates[candidate.unique_identifier()] = candidate
    for identifier, candidate in candidates.items():
        dict_ = {
            "text": f"{reference.html_comparaison_table(candidate)}",
            "entity": entity.dict(),
            "reference_1": reference.dict(),
            "reference_2": candidate.dict(),
        }
        with open("data.jsonl", "a") as f:
            f.write(json.dumps(dict_) + "\n")


def extract_information(message) -> tuple[Entity, Reference]:
    parsed_json = ast.literal_eval(message.body.decode("utf-8"))
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
