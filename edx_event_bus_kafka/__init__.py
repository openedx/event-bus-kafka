"""
Kafka implementation for Open edX event bus.
"""

from openedx_events.event_bus import EventBus

from edx_event_bus_kafka.consumer.event_consumer import consume_indefinitely
from edx_event_bus_kafka.publishing.event_producer import send_to_event_bus

__version__ = '0.1.0'


class EventBusKafka(EventBus):
    """Kafka implementation of the EventBus API."""

    def __init__(config):
        ...  # TODO Read config here and create producer and consumer?

    def send(
            self, *,
            signal: OpenEdxPublicSignal, topic: str, event_key_field: str, event_data: dict, sync: bool
    ) -> None:
        return send_to_event_bus(
            signal=signal, topic=topic, event_key_field=event_key_field, event_data=event_data, sync=sync
        )

    def consume_indefinitely(self, *, topic: str, group_id: str) -> NoReturn:
        consume_indefinitely(topic=topic, group_id=group_id)


def create(config) -> EventBusKafka:
    """
    Create the Kafka implementation of the EventBus API.
    """
    return EventBusKafka(config)
