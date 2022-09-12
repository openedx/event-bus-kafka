"""
Test common configuration loading.
"""

from unittest import TestCase

from django.test.utils import override_settings

from edx_event_bus_kafka.internal import config

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
except ImportError:  # pragma: no cover
    pass


class TestSchemaRegistryClient(TestCase):
    """
    Test client creation.
    """
    def test_unconfigured(self):
        assert config.get_schema_registry_client() is None

    def test_configured(self):
        with override_settings(EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345'):
            assert isinstance(config.get_schema_registry_client(), SchemaRegistryClient)

        # Check if it's cached, too
        assert config.get_schema_registry_client() is config.get_schema_registry_client()


class TestCommonSettings(TestCase):
    """
    Test loading of settings common to producer and consumer.
    """
    def test_unconfigured(self):
        assert config.load_common_settings() is None

    def test_minimal(self):
        with override_settings(
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        ):
            assert config.load_common_settings() == {
                'bootstrap.servers': 'localhost:54321',
            }

    def test_full(self):
        with override_settings(
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                EVENT_BUS_KAFKA_API_KEY='some_other_key',
                EVENT_BUS_KAFKA_API_SECRET='some_other_secret',
        ):
            assert config.load_common_settings() == {
                'bootstrap.servers': 'localhost:54321',
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': 'some_other_key',
                'sasl.password': 'some_other_secret',
            }


class TestTopicPrefixing(TestCase):
    """
    Test autoprefixing of base topic.
    """
    def test_no_prefix(self):
        assert config.get_full_topic('user-logins') == 'user-logins'

    @override_settings(EVENT_BUS_TOPIC_PREFIX='')
    def test_empty_string_prefix(self):
        """Check that empty string is treated the same as None."""
        assert config.get_full_topic('user-logins') == 'user-logins'

    @override_settings(EVENT_BUS_TOPIC_PREFIX='stage')
    def test_regular_prefix(self):
        assert config.get_full_topic('user-logins') == 'stage-user-logins'
