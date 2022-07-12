from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from django.conf import settings
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import KafkaError, KafkaException, SerializingProducer

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_TYPE_HEADER_KEY = "ce_type"

def magic(schema, event_key_field):
    field_path = event_key_field.split(".")
    current_data = schema.init_data
    for bit in field_path:
        current_data = current_data.fields.get(bit, None)

    return None

def magic_inner(schema, field):
    if schema.name == field:
        return schema
    for thing in schema.fields:






def send_to_event_bus(signal, topic, event_key_field, **kwargs):
    """
    Send a signal event to the event bus under the specified topic

    Arguments

        signal: The original signal that sent the event
        topic: The event bus topic for the event
        event_key_field: The name of the signal data field to use as the event key
        kwargs: The event data emitted by the signal
    """
    producer = ProducerFactory.get_or_create_producer_for_signal(signal)
    event_key = magic(signal, event_key_field)
    producer.produce(topic, key=event_key, value=kwargs,
                     on_delivery=verify_event,
                     headers={EVENT_TYPE_HEADER_KEY: signal.event_type})

class ProducerFactory:
    event_type_to_producer = {}

    @staticmethod
    def create_event_data_serializer(signal):
        schema_registry_config = {
            'url': getattr(settings, 'SCHEMA_REGISTRY_URL', ''),
            'basic.auth.user.info': f"{getattr(settings, 'SCHEMA_REGISTRY_API_KEY', '')}"
                                    f":{getattr(settings, 'SCHEMA_REGISTRY_API_SECRET', '')}",
        }
        serializer = AvroSignalSerializer(signal)


        def inner_to_dict(event_data, ctx=None):  # pylint: disable=unused-argument
            return serializer.to_dict(event_data)

        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        return AvroSerializer(schema_str=serializer.schema_string(),
                              schema_registry_client=schema_registry_client,
                              to_dict=inner_to_dict)

    @staticmethod
    def create_event_key_serializer(signal, event_key_field):
        return None

    @classmethod
    def get_or_create_producer_for_signal(cls, signal, event_key_field):
        existing = cls.event_type_to_producer.get(signal.event_type, None)
        if existing:
            return existing
        data_serializer = cls.create_event_data_serializer(signal)
        key_serializer = cls.create_event_key_serializer(signal, event_key_field)

        producer_settings = {
            'bootstrap.servers': getattr(settings, 'KAFKA_BOOTSTRAP_SERVER', None),
            'key.serializer': key_serializer,
            'value.serializer': data_serializer,
        }

        if getattr(settings, 'KAFKA_API_KEY', None) and getattr(settings, 'KAFKA_API_SECRET', None):
            producer_settings.update({
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': getattr(settings, 'KAFKA_API_KEY', ''),
                'sasl.password': getattr(settings, 'KAFKA_API_SECRET', ''),
            })

        new_producer = SerializingProducer(producer_settings)
        cls._type_to_producer[signal.event_type] = new_producer
        return new_producer

def verify_event(err, evt):  # pragma: no cover
    """
    Simple callback method for debugging event production

    :param err: Error if event production failed
    :param evt: Event that was delivered
    """
    if err is not None:
        logger.warning(f"Event delivery failed: {err}")
    else:
        # Don't log msg.value() because it may contain userids and/or emails
        logger.info(f"Event delivered to {evt.topic()}: key(bytes) - {evt.key()}; "
                    f"partition - {evt.partition()}")
