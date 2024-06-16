import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from itertools import chain
from typing import List

import aio_pika
from aio_pika import ExchangeType

from commons.models import Entity, Reference, Contribution, Contributor, Result
from reports.author_report_builder import AuthorReportBuilder
from simple_duplicate_detector import SimpleDuplicateDetector
from strategies.more_like_this_similarity_strategy import MoreLikeThisSimilarityStrategy
from strategies.notice_semantic_similarity_strategy import NoticeSemanticSimilarityStrategy
from strategies.title_semantic_similarity_strategy import TitleSemanticSimilarityStrategy
from strategies.title_syntactic_similarity_strategy import TitleSyntacticSimilarityStrategy

REPORTS_DIR = "authors"

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

lines_written = 0
current_file = None
report_builders = {}


def get_new_filename():
    return f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"


def open_new_file():
    global current_file, lines_written
    lines_written = 0
    if current_file:
        current_file.close()
    current_file = open(get_new_filename(), "a")


async def main() -> None:
    # create REPORTS_DIR if it does not exist
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)
    open_new_file()
    connection = await create_connection()
    async with connection:
        queue = await create_queue(connection)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    handle_message(message)


def handle_message(message: aio_pika.IncomingMessage):
    global strategies, lines_written, report_builders
    entity, reference = extract_information(message)
    print(reference.titles)

    # If the reference has no contributions, we add the entity as an author
    if len(reference.contributions) == 0:
        reference.contributions = [
            Contribution(rank=0, role="Author",
                         contributor=Contributor(name=entity.name, source=entity.identifiers[0].type,
                                                 source_identifier=entity.identifiers[0].value, name_variants=[]))
        ]

    main_entity_id = AuthorReportBuilder.get_main_entity_id(entity)
    if main_entity_id and main_entity_id not in report_builders:
        report_builders[main_entity_id] = AuthorReportBuilder(entity=entity)
    report_builders[main_entity_id].add_reference(reference)

    raw_candidates: List[Result] = []
    for strategy in strategies:
        strategy.load_reference(entity, reference)
        raw_candidates.extend(strategy.get_similar_references(entity, reference))
    # A candidate may point to a reference that is not already attached to the entity
    for candidate in raw_candidates:
        report_builders[main_entity_id].add_reference(candidate.reference2)
    # Compute trivial duplicates
    trivial_duplicates = [candidate for candidate in raw_candidates if
                          SimpleDuplicateDetector(candidate.reference1, candidate.reference2).is_duplicate()]

    for candidate in trivial_duplicates:
        report_builders[main_entity_id].add_trivial_duplicate(candidate.reference1, candidate.reference2)

    # If a candidate is in trivial_duplicates, remove it from raw_candidates

    raw_candidates = [candidate for candidate in raw_candidates if
                      (candidate.reference1.unique_identifier(), candidate.reference2.unique_identifier()) not in
                      report_builders[main_entity_id].get_trivial_duplicates()]

    # if not already present
    for candidate in raw_candidates:
        report_builders[main_entity_id].add_potential_duplicate(candidate.reference1, candidate.reference2)

    # Merge similarity strategies and scores for the same reference
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
        if lines_written >= 100:
            open_new_file()
        current_file.write(json.dumps(dict_, default=str) + "\n")
        lines_written += 1
    report_builders[main_entity_id].dump_report(REPORTS_DIR)


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
