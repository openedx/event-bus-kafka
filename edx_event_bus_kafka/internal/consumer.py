"""
Core consumer and event-loop code.
"""

import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from edx_toggles.toggles import SettingToggle
from openedx_events.event_bus.avro.deserializer import AvroSignalDeserializer
from openedx_events.tooling import OpenEdxPublicSignal

from .config import get_full_topic, get_schema_registry_client, load_common_settings

logger = logging.getLogger(__name__)

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    import confluent_kafka
    from confluent_kafka import DeserializingConsumer, KafkaError
    from confluent_kafka.schema_registry.avro import AvroDeserializer
except ImportError:  # pragma: no cover
    confluent_kafka = None

# .. toggle_name: EVENT_BUS_KAFKA_CONSUMERS_ENABLED
# .. toggle_implementation: SettingToggle
# .. toggle_default: True
# .. toggle_description: If set to False, consumer will exit immediately. This can be used as an emergency kill-switch
#   to disable a consumer—as long as the management command is killed and restarted when settings change.
# .. toggle_use_cases: opt_out
# .. toggle_creation_date: 2022-01-31
KAFKA_CONSUMERS_ENABLED = SettingToggle('EVENT_BUS_KAFKA_CONSUMERS_ENABLED', default=True)

CONSUMER_POLL_TIMEOUT = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT', 1.0)

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER = "ce_type"


class KafkaEventConsumer:
    """
    Construct consumer for the given topic, group, and signal. The consumer can then
    emit events from the event bus using the configured signal.

    Can also consume messages indefinitely off the queue.
    """

    def __init__(self, topic, group_id, signal):
        if confluent_kafka is None:  # pragma: no cover
            raise Exception('Library confluent-kafka not available. Cannot create event consumer.')

        self.topic = topic
        self.group_id = group_id
        self.signal = signal
        self.consumer = self._create_consumer()

    # return type (Optional[DeserializingConsumer]) removed from signature to avoid error on import
    def _create_consumer(self):
        """
        Create a DeserializingConsumer for events of the given signal instance.

        Returns
            None if confluent_kafka is not available.
            DeserializingConsumer if it is.
        """

        schema_registry_client = get_schema_registry_client()

        signal_deserializer = AvroSignalDeserializer(self.signal)

        def inner_from_dict(event_data_dict, ctx=None):  # pylint: disable=unused-argument
            return signal_deserializer.from_dict(event_data_dict)

        consumer_config = load_common_settings()

        # We do not deserialize the key because we don't need it for anything yet.
        # Also see https://github.com/openedx/openedx-events/issues/86 for some challenges on determining key schema.
        consumer_config.update({
            'group.id': self.group_id,
            'value.deserializer': AvroDeserializer(schema_str=signal_deserializer.schema_string(),
                                                   schema_registry_client=schema_registry_client,
                                                   from_dict=inner_from_dict),
        })

        return DeserializingConsumer(consumer_config)

    def consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop.
        """
        # This is already checked at the Command level, but it's possible this loop
        # could get called some other way, so check it here too.
        if not KAFKA_CONSUMERS_ENABLED.is_enabled():
            logger.error("Kafka consumers not enabled")
            return

        try:
            full_topic = get_full_topic(self.topic)
            self.consumer.subscribe([full_topic])

            # TODO: Make sure exceptions won't kill the loop. https://github.com/openedx/event-bus-kafka/issues/62
            while True:
                msg = self.consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)
                if msg is not None:
                    self.process_single_message(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()
            logger.info("Committing final offsets")

    def process_single_message(self, msg):
        """
        Emit signal with message data
        """
        if msg.error():
            # TODO: Iterate on error handling with retry and dead-letter queue topics.
            # https://github.com/edx/edx-arch-experiments/issues/55 has broad overview of questions about errors.
            if msg.error().code() == KafkaError._PARTITION_EOF:  # pylint: disable=protected-access
                # End of partition event
                logger.info(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset}")
            else:
                logger.exception(msg.error())
        else:
            self.emit_signals_from_message(msg)

    def emit_signals_from_message(self, msg):
        """
        Determine the correct signal and send the event from the message.

        Arguments:
            msg (Message): Consumed message.
        """
        headers = msg.headers() or []  # treat None as []

        event_types = [value for key, value in headers if key == EVENT_TYPE_HEADER]
        if len(event_types) == 0:
            logger.error(f"Missing {EVENT_TYPE_HEADER} header on message, cannot determine signal")
            return
        if len(event_types) > 1:
            logger.error(f"Multiple {EVENT_TYPE_HEADER} headers found on message, cannot determine signal")
            return

        event_type = event_types[0]

        # CloudEvents specifies using UTF-8 for header values, so let's be explicit.
        event_type_str = event_type.decode("utf-8")

        # TODO: Maybe raise error here? Or at least set a metric or custom attribute.
        if event_type_str != self.signal.event_type:
            logger.error(
                f"Signal types do not match. Expected {self.signal.event_type}."
                f"Received message of type {event_type_str}."
            )
            return

        self.signal.send_event(**msg.value())


class ConsumeEventsCommand(BaseCommand):
    """
    Management command for Kafka consumer workers in the event bus.
    """
    help = """
    Consume messages of specified signal type from a Kafka topic and send their data to that signal.

    example:
        python3 manage.py cms consume_events -t user-login -g user-activity-service \
            -s org.openedx.learning.auth.session.login.completed.v1
    """

    def add_arguments(self, parser):

        parser.add_argument(
            '-t', '--topic',
            nargs=1,
            required=True,
            help='Topic to consume (without environment prefix)'
        )

        parser.add_argument(
            '-g', '--group_id',
            nargs=1,
            required=True,
            help='Consumer group id'
        )
        parser.add_argument(
            '-s', '--signal',
            nargs=1,
            required=True,
            help='Type of signal to emit from consumed messages.'
        )

    def handle(self, *args, **options):
        if not KAFKA_CONSUMERS_ENABLED.is_enabled():
            logger.error("Kafka consumers not enabled")
            return
        try:
            signal = OpenEdxPublicSignal.get_signal_by_type(options['signal'][0])
            event_consumer = KafkaEventConsumer(
                topic=options['topic'][0],
                group_id=options['group_id'][0],
                signal=signal,
            )
            event_consumer.consume_indefinitely()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error consuming Kafka events")
