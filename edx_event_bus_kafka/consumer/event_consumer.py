"""
Core consumer and event-loop code.
"""

import logging

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.dispatch import receiver
from edx_toggles.toggles import SettingToggle
from openedx_events.event_bus.avro.deserializer import AvroSignalDeserializer
from openedx_events.learning.data import UserData
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED
from openedx_events.tooling import OpenEdxPublicSignal

logger = logging.getLogger(__name__)

# .. toggle_name: KAFKA_CONSUMERS_ENABLED
# .. toggle_implementation: SettingToggle
# .. toggle_default: False
# .. toggle_description: Enables the ability to listen and process events from the Kafka event bus
# .. toggle_use_cases: opt_in
# .. toggle_creation_date: 2022-01-31
# .. toggle_tickets: https://openedx.atlassian.net/browse/ARCHBOM-1992
KAFKA_CONSUMERS_ENABLED = SettingToggle('KAFKA_CONSUMERS_ENABLED', default=False)

CONSUMER_POLL_TIMEOUT = getattr(settings, 'CONSUMER_POLL_TIMEOUT', 1.0)

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER = "ce_type"


def create_consumer(group_id) -> DeserializingConsumer:
    """
    Create a consumer for SESSION_LOGIN_COMPLETED events.

    Note: Still needs to be expanded to cover arbitrary events.

    Arguments:
        group_id: id of the consumer group this consumer will be part of
    """

    # TODO (EventBus): Deduplicate settings/client construction against producer code.
    KAFKA_SCHEMA_REGISTRY_CONFIG = {
        'url': settings.SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f"{getattr(settings, 'SCHEMA_REGISTRY_API_KEY', '')}"
                                f":{getattr(settings, 'SCHEMA_REGISTRY_API_SECRET', '')}",
    }

    schema_registry_client = SchemaRegistryClient(KAFKA_SCHEMA_REGISTRY_CONFIG)

    # TODO (EventBus):
    # 1. Reevaluate if all consumers should listen for the earliest unprocessed offset (auto.offset.reset)
    # 2. Ensure the signal used in the signal_deserializer is the same one sent over in the message header

    signal_deserializer = AvroSignalDeserializer(SESSION_LOGIN_COMPLETED)

    def inner_from_dict(event_data_dict, ctx=None):  # pylint: disable=unused-argument
        return signal_deserializer.from_dict(event_data_dict)

    consumer_config = {
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'key.deserializer': StringDeserializer('utf-8'),
        'value.deserializer': AvroDeserializer(schema_str=signal_deserializer.schema_string(),
                                               schema_registry_client=schema_registry_client,
                                               from_dict=inner_from_dict),
        'auto.offset.reset': 'earliest'
    }

    if getattr(settings, 'KAFKA_API_KEY', None) and getattr(settings, 'KAFKA_API_SECRET', None):
        consumer_config.update({
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        })

    return DeserializingConsumer(consumer_config)


def emit_signals_from_message(msg):
    """
    Determine the correct signal and send the event from the message.

    Arguments:
        msg (Message): Consumed message.
    """
    headers = msg.headers() or []  # treat None as []

    # TODO (EventBus): iterate on error handling for missing or multiple event_type headers
    #  (headers() is actually a list of (key, value) tuples rather than a dictionary)
    event_types = [value for key, value in headers if key == EVENT_TYPE_HEADER]
    if len(event_types) == 0:
        logger.error(f"Missing {EVENT_TYPE_HEADER} header on message, cannot determine signal")
        return
    if len(event_types) > 1:
        logger.error(f"Multiple {EVENT_TYPE_HEADER} headers found on message, cannot determine signal")
        return

    event_type = event_types[0]

    # TODO (EventBus): Figure out who is doing the encoding and get the
    #  right one instead of just guessing utf-8
    event_type_str = event_type.decode("utf-8")
    try:
        signal = OpenEdxPublicSignal.get_signal_by_type(event_type_str)
        signal.send_event(**msg.value())
    except KeyError:
        logger.exception(f"Signal not found: {event_type_str}")


def process_single_message(msg):
    """
    Emit signal with message data
    """
    if msg.error():
        # TODO (EventBus): iterate on error handling with retry and dead-letter queue topics
        if msg.error().code() == KafkaError._PARTITION_EOF:  # pylint: disable=protected-access
            # End of partition event
            logger.info(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset}")
        else:
            logger.exception(msg.error())
    else:
        emit_signals_from_message(msg)


def consume_indefinitely(topic, group_id):
    """
    Consume events from a topic in an infinite loop.
    """
    consumer = create_consumer(group_id)

    try:
        consumer.subscribe([topic])

        # TODO (EventBus):
        # 1. Is there an elegant way to exit the loop?
        # 2. Determine if there are other errors that shouldn't kill the entire loop
        while True:
            msg = consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)
            if msg is not None:
                process_single_message(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        logger.info("Committing final offsets")


class ConsumeEventsCommand(BaseCommand):
    """
    Listen for events from the event bus and log them. Only run on servers where KAFKA_CONSUMERS_ENABLED is true
    """
    help = """
    This starts a Kafka event consumer that listens to the specified topic and logs all messages it receives. Topic
    is required.

    example:
        python3 manage.py cms consume_events -t user-event-debug -g user-event-consumers

    # TODO (EventBus): Add pointer to relevant future docs around topics and consumer groups, and potentially
    update example topic and group names to follow any future naming conventions.

    """

    def add_arguments(self, parser):

        parser.add_argument(
            '-t', '--topic',
            nargs=1,
            required=True,
            help='Topic to consume'
        )

        parser.add_argument(
            '-g', '--group_id',
            nargs=1,
            required=True,
            help='Consumer group id'
        )

    def handle(self, *args, **options):
        if not KAFKA_CONSUMERS_ENABLED.is_enabled():
            logger.error("Kafka consumers not enabled")
            return
        try:
            consume_indefinitely(
                topic=options['topic'][0],
                group_id=options['group_id'][0],
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error consuming Kafka events")


@receiver(SESSION_LOGIN_COMPLETED)
def log_event_from_event_bus(**kwargs):  # pragma: no cover
    """
    Log event received and transmitted from event bus consumer.

    This is test code that should be removed.

    Arguments:
        kwargs: event data sent to the signal
    """
    try:
        user_data = kwargs.get('user', None)
        if not user_data or not isinstance(user_data, UserData):
            logger.error("Received null or incorrect data from SESSION_LOGIN_COMPLETED")
            return
        logger.info(f"Received SESSION_LOGIN_COMPLETED signal with user_data"
                    f" with UserData {user_data}")
    except Exception:  # pylint: disable=broad-except
        logger.exception("Error while testing receiving signals from Kafka events")
