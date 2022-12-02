"""
Produce Kafka events from signals.

Main function is ``create_producer()``, which should be referred to from ``EVENT_BUS_PRODUCER``.
"""

import json
import logging
import threading
import time
import weakref
from functools import lru_cache
from typing import Any, List, Optional

import attr
from django.conf import settings
from django.dispatch import receiver
from django.test.signals import setting_changed
from edx_django_utils.monitoring import record_exception
from openedx_events.data import EventsMetadata
from openedx_events.event_bus import EventBusProducer
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from openedx_events.tooling import OpenEdxPublicSignal

from .config import get_full_topic, get_schema_registry_client, load_common_settings

logger = logging.getLogger(__name__)

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    import confluent_kafka
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
except ImportError:  # pragma: no cover
    confluent_kafka = None

# CloudEvent standard names for the event headers, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER_KEY = "ce_type"
ID_HEADER_KEY = "ce_id"
SOURCE_HEADER_KEY = "ce_source"
SOURCEHOST_HEADER_KEY = "sourcehost"
SPEC_VERSION_HEADER_KEY = "ce_specversion"
# The documentation is unclear as to which of the following two headers to use for content type, so for now
# use both
CONTENT_TYPE_HEADER_KEY = "content-type"
DATA_CONTENT_TYPE_HEADER_KEY = "ce_datacontenttype"


def record_producing_error(error, context):
    """
    Record an error in producing an event to both the monitoring system and the regular logs

    Arguments:
        error: The exception or error raised during producing
        context: An instance of ProducingContext containing additional information about the message
    """
    try:
        # record_exception() is a wrapper around a New Relic method that can only be called within an except block,
        # so first re-raise the error
        raise Exception(error)
    except BaseException:
        record_exception()
        logger.exception(f"Error producing event to event bus. {error=!s} {context!r}")


def extract_event_key(event_data: dict, event_key_field: str) -> Any:
    """
    From an event object, extract a Kafka event key (not yet serialized).

    Arguments:
        event_data: The event data (kwargs) sent to the signal
        event_key_field: Path to the event data field to use as the event key (period-delimited
          string naming the dictionary keys to descend)

    Returns:
        Key data, which might be an integer, string, dictionary, etc.
    """
    field_path = event_key_field.split(".")
    current_data = event_data
    for field_name in field_path:
        if isinstance(current_data, dict):
            if field_name not in current_data:
                raise Exception(
                    f"Could not extract key from event; lookup in {event_key_field} "
                    f"failed at {field_name!r} in dictionary"
                )
            current_data = current_data[field_name]
        else:
            if not hasattr(current_data, field_name):
                raise Exception(
                    f"Could not extract key from event; lookup in {event_key_field} "
                    f"failed at {field_name!r} in object"
                )
            current_data = getattr(current_data, field_name)
    return current_data


def descend_avro_schema(serializer_schema: dict, field_path: List[str]) -> dict:
    """
    Extract a subfield within an Avro schema, recursively.

    Arguments:
        serializer_schema: An Avro schema (nested dictionaries)
        field_path: List of strings matching the 'name' of successively deeper subfields

    Returns:
        Schema for some field

    Note: Avro helpers could be moved to openedx_events.event_bus.avro.serializer to be
        used for other event bus implementations other than Kafka.
    """
    subschema = serializer_schema
    for field_name in field_path:
        try:
            # Either descend into .fields (for dictionaries) or .type.fields (for classes).
            if 'fields' not in subschema:
                # Descend through .type wrapper first
                subschema = subschema['type']
            field_list = subschema['fields']

            matching = [field for field in field_list if field['name'] == field_name]
            subschema = matching[0]
        except Exception as e:
            raise Exception(
                f"Error traversing Avro schema along path {field_path!r}; failed at {field_name!r}."
            ) from e
    return subschema


def extract_key_schema(signal_serializer: AvroSignalSerializer, event_key_field: str) -> str:
    """
    From a signal's serializer, extract just the part of the Avro schema that will be used for the Kafka event key.

    Arguments:
        signal_serializer: The signal serializer to extract a sub-schema from
        event_key_field: Path to the event data field to use as the event key (period-delimited
          string naming the dictionary keys to descend)

    Returns:
        The key's schema, as a string.
    """
    subschema = descend_avro_schema(signal_serializer.schema, event_key_field.split("."))
    # Same as used by AvroSignalSerializer#schema_string in openedx-events
    return json.dumps(subschema, sort_keys=True)


@lru_cache
def get_serializers(signal: OpenEdxPublicSignal, event_key_field: str):
    """
    Get the key and value serializers for a signal and a key field path.

    This is cached in order to save work re-transforming classes into Avro schemas.

    Arguments:
        signal: The OpenEdxPublicSignal to make a serializer for.
        event_key_field: Path to descend in the signal schema to find the subschema for the key
          (period-delimited string naming the field names to descend).

    Returns:
        2-tuple of AvroSignalSerializers, for event key and value
    """
    client = get_schema_registry_client()
    if client is None:
        raise Exception('Cannot create Kafka serializers -- missing library or settings')

    signal_serializer = AvroSignalSerializer(signal)

    def inner_to_dict(event_data, ctx=None):  # pylint: disable=unused-argument
        """Tells Avro how to turn objects into dictionaries."""
        return signal_serializer.to_dict(event_data)

    # Serializers for key and value components of Kafka event
    key_serializer = AvroSerializer(
        schema_str=extract_key_schema(signal_serializer, event_key_field),
        schema_registry_client=client,
        to_dict=inner_to_dict,
    )
    value_serializer = AvroSerializer(
        schema_str=signal_serializer.schema_string(),
        schema_registry_client=client,
        to_dict=inner_to_dict,
    )

    return key_serializer, value_serializer


def _get_headers_from_metadata(event_metadata: EventsMetadata):
    """
    Create a dictionary of CloudEvent-compliant Kafka headers from an EventsMetadata object.

    This method assumes the EventMetadata object was the one sent with the event data to the original signal handler.

    Arguments:
        event_metadata: An EventsMetadata object sent by an OpenEdxPublicSignal

    Returns:
        A dictionary of headers
    """
    # Dictionary (or list of key/value tuples) where keys are strings and values are binary.
    # CloudEvents specifies using UTF-8; that should be the default, but let's make it explicit.
    return {
        # The way EventMetadata is initialized none of these should ever be null.
        # If it is we want the error to be raised.
        EVENT_TYPE_HEADER_KEY: event_metadata.event_type.encode("utf-8"),
        ID_HEADER_KEY: str(event_metadata.id).encode("utf-8"),
        SOURCE_HEADER_KEY: event_metadata.source.encode("utf-8"),
        SOURCEHOST_HEADER_KEY: event_metadata.sourcehost.encode("utf-8"),
        # Always 1.0. See "Fields" in OEP-41
        SPEC_VERSION_HEADER_KEY: b'1.0',
        CONTENT_TYPE_HEADER_KEY: b'application/avro',
        DATA_CONTENT_TYPE_HEADER_KEY: b'application/avro',
    }


@attr.s(kw_only=True, repr=False)
class ProducingContext:
    """
    Wrapper class to allow us to link a call to produce() with the on_event_deliver callback
    """
    full_topic = attr.ib(type=str, default=None)
    event_key = attr.ib(type=str, default=None)
    signal = attr.ib(type=OpenEdxPublicSignal, default=None)
    initial_topic = attr.ib(type=str, default=None)
    event_key_field = attr.ib(type=str, default=None)
    event_data = attr.ib(type=dict, default=None)
    event_metadata = attr.ib(type=EventsMetadata, default=None)

    def __repr__(self):
        """Create a logging-friendly string"""
        return " ".join([f"{key}={value!r}" for key, value in attr.asdict(self, recurse=False).items()])

    def on_event_deliver(self, err, evt):
        """
        Simple callback method for debugging event production

        If there is any error, log all the known information about the calling context so the event can be recreated
        and/or resent later. This log will not contain the exact headers but will contain the EventsMetadata object
        that can be used to recreate them.

        Arguments:
            err: Error if event production failed
            evt: Event that was delivered (or failed to be delivered)
        """
        if err is not None:
            record_producing_error(err, self)
        else:
            logger.info(f"Event delivered to topic {evt.topic()}; key={evt.key()}; "
                        f"partition={evt.partition()}")


class KafkaEventProducer(EventBusProducer):
    """
    API singleton for event production to Kafka.

    This is just a wrapper around a confluent_kafka Producer that knows how to
    serialize a signal to event wire format.

    Only one instance (of Producer or this wrapper) should be created,
    since it is stateful and needs lifecycle management.
    """

    def __init__(self, producer):
        self.producer = producer

        threading.Thread(
            target=poll_indefinitely,
            name="kafka-producer-poll",
            args=(weakref.ref(self),),  # allow GC but also thread auto-stop (important for tests!)
            daemon=True,  # don't block shutdown
        ).start()

    def send(
            self, *, signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
            event_metadata: EventsMetadata
    ) -> None:
        """
        Send a signal event to the event bus under the specified topic.

        Arguments:
            signal: The original OpenEdxPublicSignal the event was sent to
            topic: The base (un-prefixed) event bus topic for the event
            event_key_field: Path to the event data field to use as the event key (period-delimited
              string naming the dictionary keys to descend)
            event_data: The event data (kwargs) sent to the signal
            event_metadata: An EventsMetadata object with all the metadata necessary for the CloudEvent spec
        """

        # keep track of the initial arguments for recreating the event in the logs if necessary later
        context = ProducingContext(signal=signal, initial_topic=topic, event_key_field=event_key_field,
                                   event_data=event_data, event_metadata=event_metadata)
        try:
            full_topic = get_full_topic(topic)
            context.full_topic = full_topic

            event_key = extract_event_key(event_data, event_key_field)
            context.event_key = event_key
            headers = _get_headers_from_metadata(event_metadata=event_metadata)

            key_serializer, value_serializer = get_serializers(signal, event_key_field)
            key_bytes = key_serializer(event_key, SerializationContext(full_topic, MessageField.KEY, headers))
            value_bytes = value_serializer(event_data, SerializationContext(full_topic, MessageField.VALUE, headers))
            self.producer.produce(
                full_topic, key=key_bytes, value=value_bytes, headers=headers,
                on_delivery=context.on_event_deliver,
            )

            # Opportunistically ensure any pending callbacks from recent event-sends are triggered.
            # This ensures that we're polling at least as often as we're producing, which is a
            # reasonable balance. However, if events are infrequent, it doesn't ensure that
            # callbacks happen in a timely fashion, and the last event emitted before shutdown
            # would never get a delivery callback. That's why there's also a thread calling
            # poll(0) on a regular interval (see `poll_indefinitely`).
            self.producer.poll(0)
        except Exception as e:  # pylint: disable=broad-except
            # Errors caused by the produce call should be handled by the on_delivery callback.
            # Here we might expect serialization errors, or any errors from preparing to produce.
            record_producing_error(e, context)

    def prepare_for_shutdown(self):
        """
        Prepare producer for a clean shutdown.

        Flush pending outbound events, wait for acknowledgement, and process callbacks.
        """
        self.producer.flush(-1)


def poll_indefinitely(api_weakref: KafkaEventProducer):
    """
    Poll the producer indefinitely to ensure delivery/stats/etc. callbacks are triggered.

    The thread stops automatically once the producer is garbage-collected.

    See ADR for more information:
    https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0007-producer-polling.rst
    """
    # The reason we hold a weakref to the whole KafkaEventProducer and
    # not directly to the Producer itself is that you just can't make
    # a weakref to the latter (perhaps because it's a C object.)

    # .. setting_name: EVENT_BUS_KAFKA_POLL_INTERVAL_SEC
    # .. setting_default: 1.0
    # .. setting_description: How frequently to poll the event-bus-kafka producer. This should
    #   be small enough that there's not too much latency in triggering delivery callbacks once
    #   a message has been acknowledged, but there's no point in setting it any lower than the
    #   expected round-trip-time of message delivery and acknowledgement. (100 ms – 5 s is
    #   probably a reasonable range.)
    poll_interval_seconds = getattr(settings, 'EVENT_BUS_KAFKA_POLL_INTERVAL_SEC', 1.0)
    while True:
        time.sleep(poll_interval_seconds)

        # Temporarily hold a strong ref to the producer API singleton
        api_object = api_weakref()
        if api_object is None:
            return
        try:
            api_object.producer.poll(0)
        except BaseException:
            logger.exception("Event bus producer polling loop encountered exception (continuing)")
            record_exception()
        finally:
            # Get rid of that strong ref again
            api_object = None


def create_producer() -> Optional[KafkaEventProducer]:
    """
    Create a Producer API instance. Caller should cache the returned object.

    If confluent-kafka library or essential settings are missing, warn and return None.
    """
    if not confluent_kafka:  # pragma: no cover
        logger.warning('Library confluent-kafka not available. Cannot create event producer.')
        return None

    producer_settings = load_common_settings()
    if producer_settings is None:
        return None

    return KafkaEventProducer(Producer(producer_settings))


@receiver(setting_changed)
def _reset_caches(sender, **kwargs):  # pylint: disable=unused-argument
    """Reset caches when settings change during unit tests."""
    get_serializers.cache_clear()
