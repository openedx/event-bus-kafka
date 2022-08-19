"""
Configuration loading and validation.
"""

import warnings
from typing import Optional

from django.conf import settings

try:
    import confluent_kafka
    from confluent_kafka.schema_registry import SchemaRegistryClient
except ImportError:
    confluent_kafka = None


# return type (Optional[SchemaRegistryClient]) removed from signature to avoid error on import
def create_schema_registry_client():
    """
    Create a schema registry client from common settings.

    Returns
        None if confluent_kafka library is not available or the settings are invalid.
        SchemaRegistryClient if it is.
    """
    if not confluent_kafka:
        warnings.warn('Library confluent-kafka not available. Cannot create schema registry client.')
        return None

    url = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL', None)
    if url is None:
        warnings.warn("Cannot configure event-bus-kafka: Missing setting EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL")
        return None

    key = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY', '')
    secret = getattr(settings, 'EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET', '')

    return SchemaRegistryClient({
        'url': url,
        'basic.auth.user.info': f"{key}:{secret}",
    })


def load_common_settings() -> Optional[dict]:
    """
    Load common settings, a base for either producer or consumer configuration.
    """
    bootstrap_servers = getattr(settings, 'EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS', None)
    if bootstrap_servers is None:
        warnings.warn("Cannot configure event-bus-kafka: Missing setting EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS")
        return None

    base_settings = {
        'bootstrap.servers': bootstrap_servers,
    }

    key = getattr(base_settings, 'EVENT_BUS_KAFKA_API_KEY', None)
    secret = getattr(base_settings, 'EVENT_BUS_KAFKA_API_SECRET', None)

    if key and secret:
        base_settings.update({
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': key,
            'sasl.password': secret,
        })

    return base_settings
