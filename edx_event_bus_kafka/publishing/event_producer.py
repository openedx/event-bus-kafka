"""
Produce Kafka events from signals.
"""

import json
import logging
from functools import lru_cache

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from django.conf import settings
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer

logger = logging.getLogger(__name__)

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER_KEY = "ce_type"


def extract_event_key(event_data, event_key_field):
    """
    From an event object, extract a Kafka event key (not yet serialized).
    """
    field_path = event_key_field.split(".")
    current_data = event_data
    for bit in field_path:
        if isinstance(current_data, dict):
            if bit not in current_data:
                raise Exception(
                    f"Could not extract key from event; lookup in {event_key_field} "
                    f"failed at {bit!r} in dictionary"
                )
            current_data = current_data[bit]
        else:
            if not hasattr(current_data, bit):
                raise Exception(
                    f"Could not extract key from event; lookup in {event_key_field} "
                    f"failed at {bit!r} in object"
                )
            current_data = getattr(current_data, bit)
    return current_data


def descend_avro_schema(serializer_schema, field_path):
    """
    Extract a subfield within an Avro schema, recursively.

        serializer_schema: An Avro schema (nested dictionaries)
        field_path: List of strings matching the 'name' of subfields

    TODO: Move to openedx_events.event_bus.avro.serializer?
    """
    subschema = serializer_schema
    for bit in field_path:
        try:
            # Either descend into .fields (for dictionaries) or .type.fields (for classes).
            if 'fields' not in subschema:
                # Descend through .type wrapper first
                subschema = subschema['type']
            field_list = subschema['fields']

            matching = [field for field in field_list if field['name'] == bit]
            subschema = matching[0]
        except BaseException as e:
            raise Exception(
                f"Error traversing Avro schema along path {field_path!r}; failed at {bit!r}."
            ) from e
    return subschema


def extract_key_schema(signal_serializer, event_key_field):
    """
    From a signal's serializer, extract just the part of the Avro schema that will be used for the Kafka event key.

    Returns the (sub-)schema as a string.
    """
    subschema = descend_avro_schema(signal_serializer.schema, event_key_field.split("."))
    # Same as used by AvroSignalSerializer#schema_string in openedx-events
    return json.dumps(subschema, sort_keys=True)


@lru_cache
def get_producer_for_signal(signal, event_key_field):
    """
    Create the producer for a signal and a key field path.
    """
    schema_registry_config = {
        'url': getattr(settings, 'SCHEMA_REGISTRY_URL', ''),
        'basic.auth.user.info': f"{getattr(settings, 'SCHEMA_REGISTRY_API_KEY', '')}"
                                f":{getattr(settings, 'SCHEMA_REGISTRY_API_SECRET', '')}",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    signal_serializer = AvroSignalSerializer(signal)

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

    producer_settings = {
        'bootstrap.servers': getattr(settings, 'KAFKA_BOOTSTRAP_SERVER', None),
        'key.serializer': key_serializer,
        'value.serializer': value_serializer,
    }

    if getattr(settings, 'KAFKA_API_KEY', None) and getattr(settings, 'KAFKA_API_SECRET', None):
        producer_settings.update({
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': getattr(settings, 'KAFKA_API_KEY', ''),
            'sasl.password': getattr(settings, 'KAFKA_API_SECRET', ''),
        })

    return SerializingProducer(producer_settings)


def verify_event(err, evt):
    """
    Simple callback method for debugging event production

    :param err: Error if event production failed
    :param evt: Event that was delivered
    """
    if err is not None:
        logger.warning(f"Event delivery failed: {err!r}")
    else:
        # Don't log msg.value() because it may contain userids and/or emails
        logger.info(f"Event delivered to topic {evt.topic()}; key={evt.key()}; "
                    f"partition={evt.partition()}")


def send_to_event_bus(signal, topic, event_key_field, event_data):
    """
    Send a signal event to the event bus under the specified topic.

    :param signal: The original OpenEdxPublicSignal the event was sent to
    :param topic: The event bus topic for the event
    :param event_key_field: The name of the signal data field to use as the
       event key (dot-separated path of dictionary key/attribute names)
    :param event_data: The data sent to the signal
    """
    producer = get_producer_for_signal(signal, event_key_field)
    event_key = extract_event_key(event_data, event_key_field)
    producer.produce(topic, key=event_key, value=event_data,
                     on_delivery=verify_event,
                     headers={EVENT_TYPE_HEADER_KEY: signal.event_type})
