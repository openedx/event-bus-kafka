"""
Produce Kafka events from signals.

Main function is ``send_to_event_bus``.
"""

import json
import logging
from functools import lru_cache
from typing import Any, List

from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from openedx_events.tooling import OpenEdxPublicSignal

from edx_event_bus_kafka.config import create_schema_registry_client, load_common_settings

logger = logging.getLogger(__name__)

try:
    import confluent_kafka
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry.avro import AvroSerializer
except ImportError:
    confluent_kafka = None

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER_KEY = "ce_type"


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
        except BaseException as e:
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
def get_serializer(signal: OpenEdxPublicSignal) -> AvroSignalSerializer:
    """
    Get the serializer for a signal.

    This is just defined to allow caching of serializers.
    """
    return AvroSignalSerializer(signal)


# Note: This caching is required, since otherwise the Producer will
# fall out of scope and be garbage-collected, destroying the
# outbound-message queue and threads. The use of this cache allows the
# producers to be long-lived.

# return type (Optional[SerializingProducer]) removed from signature to avoid error on import

@lru_cache
def get_producer_for_signal(signal: OpenEdxPublicSignal, event_key_field: str):
    """
    Create the producer for a signal and a key field path.

    If essential settings are missing or invalid, warn and return None.

    Arguments:
        signal: The OpenEdxPublicSignal to make a producer for
        event_key_field: Path to the event data field to use as the event key (period-delimited
          string naming the dictionary keys to descend)
    Returns:
        None if confluent_kafka is not defined or the settings are invalid.
        SerializingProducer if it is.

    Performance note:
        This could be cached, but requires care such that it allows changes to settings via
        remote-config (and in particular does not result in mixed cache/uncached configuration).
        This complexity is being deferred until this becomes a performance issue.
    """
    if not confluent_kafka:
        logger.warning('Library confluent-kafka not available. Cannot create event producer.')
        return None

    schema_registry_client = create_schema_registry_client()
    if schema_registry_client is None:
        return None

    producer_settings = load_common_settings()
    if producer_settings is None:
        return None

    signal_serializer = get_serializer(signal)

    def inner_to_dict(event_data, ctx=None):  # pylint: disable=unused-argument
        """Tells Avro how to turn objects into dictionaries."""
        return signal_serializer.to_dict(event_data)

    # Serializers for key and value components of Kafka event
    key_serializer = AvroSerializer(
        schema_str=extract_key_schema(signal_serializer, event_key_field),
        schema_registry_client=schema_registry_client,
        to_dict=inner_to_dict,
    )
    value_serializer = AvroSerializer(
        schema_str=signal_serializer.schema_string(),
        schema_registry_client=schema_registry_client,
        to_dict=inner_to_dict,
    )

    producer_settings.update({
        'key.serializer': key_serializer,
        'value.serializer': value_serializer,
    })

    return SerializingProducer(producer_settings)


def on_event_deliver(err, evt):
    """
    Simple callback method for debugging event production

    Arguments:
        err: Error if event production failed
        evt: Event that was delivered (or failed to be delivered)

    Note: This is meant to be temporary until we implement
      more rigorous error handling.
    """
    if err is not None:
        logger.warning(f"Event delivery failed: {err!r}")
    else:
        # Don't log msg.value() because it may contain userids and/or emails
        logger.info(f"Event delivered to topic {evt.topic()}; key={evt.key()}; "
                    f"partition={evt.partition()}")


def send_to_event_bus(
        signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict,
        sync: bool = False,
) -> None:
    """
    Send a signal event to the event bus under the specified topic.

    If the Kafka settings are missing or invalid, return with a warning.

    Arguments:
        signal: The original OpenEdxPublicSignal the event was sent to
        topic: The event bus topic for the event
        event_key_field: Path to the event data field to use as the event key (period-delimited
          string naming the dictionary keys to descend)
        event_data: The event data (kwargs) sent to the signal
        sync: Whether to wait indefinitely for event to be received by the message bus (probably
          only want to use this for testing)
    """
    producer = get_producer_for_signal(signal, event_key_field)
    if producer is None:  # Note: SerializingProducer has False truthiness when len() == 0
        return

    event_key = extract_event_key(event_data, event_key_field)
    producer.produce(topic, key=event_key, value=event_data,
                     on_delivery=on_event_deliver,
                     headers={EVENT_TYPE_HEADER_KEY: signal.event_type})

    if sync:
        # Wait for all buffered events to send, then wait for all of
        # them to be acknowledged, and trigger all callbacks.
        producer.flush(-1)
    else:
        # Opportunistically ensure any pending callbacks from recent events are triggered.
        #
        # This assumes events come regularly, or that we're not concerned about
        # high latency between delivery and callback. If those assumptions are
        # false, we should switch to calling poll(1.0) or similar in a loop on
        # a separate thread.
        #
        # Docs: https://github.com/edenhill/librdkafka/blob/4faeb8132521da70b6bcde14423a14eb7ed5c55e/src/rdkafka.h#L3079
        producer.poll(0)
