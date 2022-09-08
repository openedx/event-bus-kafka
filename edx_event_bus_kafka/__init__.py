"""
Kafka implementation for Open edX event bus.

Public API will be in this module for the most part.

See ADR ``docs/decisions/0006-public-api-and-app-organization.rst`` for the reasoning.
"""

from edx_event_bus_kafka.internal.producer import EventProducerKafka, get_producer
from edx_event_bus_kafka.internal.consumer import KafkaEventConsumer

__version__ = '0.6.3'
