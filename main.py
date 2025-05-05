import asyncio
import json
import os
from datetime import datetime
from typing import List

import aio_pika
from aio_pika import ExchangeType
from aiohttp import web

from commons.models import Entity, Reference, Contribution, Contributor, Result
from exclusion_filter import ExclusionFilter
from reports.author_report_builder import AuthorReportBuilder
from simple_duplicate_detector import SimpleDuplicateDetector
from strategies.more_like_this_similarity_strategy import MoreLikeThisSimilarityStrategy
from strategies.notice_semantic_similarity_strategy import NoticeSemanticSimilarityStrategy
from strategies.title_semantic_similarity_strategy import TitleSemanticSimilarityStrategy
from strategies.title_syntactic_similarity_strategy import TitleSyntacticSimilarityStrategy

REPORTS_DIR = "authors"
DATA_DIR = "data"

EXCHANGE_NAME = "publications"

QUEUE_NAME = "crisalid-ikg-publications"

QUEUE_TOPIC = "event.references.reference.*"

DEFAULT_AMQP_PARAMS = "amqp://guest:guest@127.0.0.1/"

strategies = [
    NoticeSemanticSimilarityStrategy(),
    TitleSemanticSimilarityStrategy(),
    TitleSyntacticSimilarityStrategy(),
    MoreLikeThisSimilarityStrategy()
]

lines_written = 0
current_file = None
report_builders = {}
rabbitmq_connected = False


def get_amqp_params():
    if all([os.getenv("AMQP_USER"), os.getenv("AMQP_PASSWORD"), os.getenv("AMQP_HOST"),
            os.getenv("AMQP_PORT")]):
        return f"amqp://{os.getenv('AMQP_USER')}:{os.getenv('AMQP_PASSWORD')}@{os.getenv('AMQP_HOST')}:{os.getenv('AMQP_PORT')}/"
    return DEFAULT_AMQP_PARAMS


# Health check endpoint
async def health_check(request):
    global rabbitmq_connected
    if rabbitmq_connected:
        return web.Response(text="OK")
    else:
        return web.Response(status=503, text="RabbitMQ connection lost")


async def start_health_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8080)
    await site.start()
    print("Health server running on port 8080")


def get_new_filename():
    return f"{DATA_DIR}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"


def open_new_file():
    global current_file, lines_written
    lines_written = 0
    if current_file:
        current_file.close()
    current_file = open(get_new_filename(), "a")


async def main() -> None:
    print("creating reports dir")
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)
    print("creating data dir")
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    print("Starting health server...")
    asyncio.create_task(start_health_server())
    open_new_file()
    connection = await create_connection()
    print("connecting")
    async with connection:
        queue = await create_queue(connection)
        print("waiting for messages")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    handle_message(message)


def handle_message(message: aio_pika.IncomingMessage):
    global strategies, lines_written, report_builders
    entity, reference = extract_information(message)
    print(reference.titles)
    if ExclusionFilter(reference).discard():
        print(f"Reference discarded  {reference.source_identifier}")
        return

    # If the reference has no contributions, we add the entity as an author
    if len(reference.contributions) == 0:
        reference.contributions = [
            Contribution(rank=0, role="Author",
                         contributor=Contributor(name=entity.name,
                                                 source=entity.identifiers[0].type,
                                                 source_identifier=entity.identifiers[0].value,
                                                 name_variants=[]))
        ]

    main_entity_id = AuthorReportBuilder.get_main_entity_id(entity)
    if main_entity_id and main_entity_id not in report_builders:
        report_builders[main_entity_id] = AuthorReportBuilder(entity=entity)
    report_builders[main_entity_id].add_reference(reference)

    raw_candidates: List[Result] = []
    for strategy in strategies:
        strategy.load_reference(entity, reference)
        raw_candidates.extend(strategy.get_similar_references(entity, reference))
    trivial_duplicates = []
    for candidate in raw_candidates:
        if SimpleDuplicateDetector(candidate.reference1, candidate.reference2).is_duplicate():
            trivial_duplicates.append(candidate)
            # A candidate may point to a reference that is not already attached to the entity
            report_builders[main_entity_id].add_reference(candidate.reference2)
        else:
            report_builders[main_entity_id].add_potential_reference(candidate.reference2)

    for candidate in trivial_duplicates:
        report_builders[main_entity_id].add_trivial_duplicate(candidate.reference1,
                                                              candidate.reference2)

    # If a candidate is in trivial_duplicates, remove it from raw_candidates

    raw_candidates = [candidate for candidate in raw_candidates if
                      (candidate.reference1.unique_identifier(),
                       candidate.reference2.unique_identifier()) not in
                      report_builders[main_entity_id].get_trivial_duplicates()]

    # if not already present
    for candidate in raw_candidates:
        report_builders[main_entity_id].add_potential_duplicate(candidate.reference1,
                                                                candidate.reference2)

    # if one of the references is a thesis from Scanr, with nnt, and the other is from idref, without nnt, but with sudoc equivalent, discard it
    # as the idref group notices does not copy the nnt identifier from sudoc
    candidates_with_missing_idref_nnt = []
    for candidate in raw_candidates:
        if not (
                candidate.reference1.harvester == 'Idref' and candidate.reference2.harvester == 'ScanR') and not (
                candidate.reference1.harvester == 'ScanR' and candidate.reference2.harvester == 'Idref'):
            continue
        ref1 = candidate.reference1 if candidate.reference1.harvester == 'Idref' else candidate.reference2
        ref2 = candidate.reference2 if candidate.reference2.harvester == 'ScanR' else candidate.reference1
        assert ref1.harvester == 'Idref'
        assert ref2.harvester == 'ScanR'
        assert not ref1.source_identifier == ref2.source_identifier
        # ref1 comes from idref, ref2 from scanr
        if not ref1.source_identifier.startswith('http://www.idref.fr/'):
            continue
        if not ref2.source_identifier.startswith('nnt'):
            continue
        # ref1 is missing nnt
        if any(identifier.type == 'nnt' for identifier in ref1.identifiers):
            continue
        candidates_with_missing_idref_nnt.append(candidate)

    raw_candidates = [candidate for candidate in raw_candidates if
                      candidate not in candidates_with_missing_idref_nnt]
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
    if any(["book" in str.lower(doctype.label) for doctype in reference.document_type]):
        print("Document type is book")
    return entity, reference


async def create_connection():
    global rabbitmq_connected

    connection = await aio_pika.connect_robust(
        get_amqp_params(),
    )

    def on_close(sender, exc):
        global rabbitmq_connected
        rabbitmq_connected = False
        print(f"RabbitMQ connection lost: {exc}")

    def on_reconnect(sender):
        global rabbitmq_connected
        rabbitmq_connected = True
        print("RabbitMQ reconnected")

    # Attach event handlers
    connection.close_callbacks.add(on_close)
    connection.reconnect_callbacks.add(on_reconnect)

    # Set initial state
    rabbitmq_connected = True
    print("RabbitMQ connection established")

    return connection


async def create_queue(connection):
    channel = await connection.channel()
    publication_exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.TOPIC,
        durable=True
    )
    await channel.set_qos(prefetch_count=os.getenv("AMQP_PREFETCH_COUNT", 10))
    queue = await channel.declare_queue(QUEUE_NAME, auto_delete=False, exclusive=False,
                                        durable=True)
    await queue.bind(publication_exchange, QUEUE_TOPIC)
    return queue


if __name__ == '__main__':
    asyncio.run(main())
