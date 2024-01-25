"""
Configuration loading and validation.

This module is for internal use only.
"""
import warnings
from functools import lru_cache
from typing import Optional

from django.conf import settings
from django.dispatch import receiver
from django.test.signals import setting_changed
from openedx_events.data import get_service_name

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    import confluent_kafka
    from confluent_kafka.schema_registry import SchemaRegistryClient
except ImportError:  # pragma: no cover
    confluent_kafka = None


# return type (Optional[SchemaRegistryClient]) removed from signature to avoid error on import
@lru_cache  # will just be one cache entry, in practice
def get_schema_registry_client():
    """
    Create a schema registry client from common settings.

    This is cached on the assumption of a performance benefit (avoid reloading settings and
    reconstructing client) but it may also be that the client keeps around long-lived
    connections that we could benefit from.

    Returns
        None if confluent_kafka library is not available or the settings are invalid.
        SchemaRegistryClient if it is.
    """
    if not confluent_kafka:  # pragma: no cover
        warnings.warn('Library confluent-kafka not available. Cannot create schema registry client.')
        return None

    # .. setting_name: EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL
    # .. setting_default: None
    # .. setting_description: URL of the Avro schema registry, required for managing the evolution
    #   of event bus data and ensuring that consumers are able to decode the events that
    #   are produced. This URL is required for both producers and consumers and must point
    #   to an instance of Confluent Schema Registry:
    #   https://docs.confluent.io/platform/current/schema-registry/index.html
    #   If needed, auth information must be added to ``EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY``
    #   and ``EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET``.
    url = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL', None)
    if url is None:
        warnings.warn("Cannot configure event-bus-kafka: Missing setting EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL")
        return None

    # .. setting_name: EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY
    # .. setting_default: ''
    # .. setting_description: API key for talking to the Avro schema registry specified in
    #   ``EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL``. Optional.
    key = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY', '')
    # .. setting_name: EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET
    # .. setting_default: ''
    # .. setting_description: API secret for talking to the Avro schema registry specified in
    #   ``EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL``. Optional.
    secret = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET', '')

    return SchemaRegistryClient({
        'url': url,
        'basic.auth.user.info': f"{key}:{secret}",
    })


def load_common_settings() -> Optional[dict]:
    """
    Load common settings, a base for either producer or consumer configuration.

    Warns and returns None if essential settings are missing.
    """
    # .. setting_name: EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS
    # .. setting_default: None
    # .. setting_description: List of one or more Kafka bootstrap servers, a comma-separated
    #   list of hosts and optional ports, and is required for both producers and consumers.
    #   See https://kafka.apache.org/documentation/ for more info.
    bootstrap_servers = getattr(settings, 'EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS', None)
    if bootstrap_servers is None:
        warnings.warn("Cannot configure event-bus-kafka: Missing setting EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS")
        return None

    base_settings = {
        'bootstrap.servers': bootstrap_servers,
    }

    # .. setting_name: EVENT_BUS_KAFKA_API_KEY
    # .. setting_default: None
    # .. setting_description: Optional API key for connecting to the Kafka cluster
    #   via SASL/PLAIN over TLS. Used as the SASL username. If not specified, no
    #   authentication will be attempted.
    key = getattr(settings, 'EVENT_BUS_KAFKA_API_KEY', None)
    # .. setting_name: EVENT_BUS_KAFKA_API_SECRET
    # .. setting_default: None
    # .. setting_description: Optional API secret for connecting to the Kafka cluster
    #   via SASL/PLAIN over TLS. Used as the SASL password. If not specified, no
    #   authentication will be attempted.
    secret = getattr(settings, 'EVENT_BUS_KAFKA_API_SECRET', None)

    if key and secret:
        base_settings.update({
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': key,
            'sasl.password': secret,
        })

    client_id = get_service_name()
    if client_id:
        base_settings.update({
            'client.id': client_id
        })
    return base_settings


def get_full_topic(base_topic: str) -> str:
    """
    Given a base topic name, add a prefix (if configured).
    """
    # .. setting_name: EVENT_BUS_TOPIC_PREFIX
    # .. setting_default: None
    # .. setting_description: If provided, add this as a prefix to any topic names (delimited by a hyphen)
    #   when either producing or consuming events. This can be used to support separation of environments,
    #   e.g. if multiple staging or test environments are sharing a cluster. For example, if the base topic
    #   name is "user-logins", then if EVENT_BUS_TOPIC_PREFIX=stage, the producer and consumer would instead
    #   work with the topic "stage-user-logins".
    topic_prefix = getattr(settings, 'EVENT_BUS_TOPIC_PREFIX', None)
    if topic_prefix:
        return f"{topic_prefix}-{base_topic}"
    else:
        return base_topic


@receiver(setting_changed)
def _reset_state(sender, **kwargs):  # pylint: disable=unused-argument
    """Reset caches when settings change during unit tests."""
    get_schema_registry_client.cache_clear()
