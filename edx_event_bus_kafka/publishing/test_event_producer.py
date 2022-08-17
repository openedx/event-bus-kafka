"""
Test the event producer code.
"""

import warnings
from unittest import TestCase
from unittest.mock import MagicMock, patch

import openedx_events.learning.signals
import pytest
from django.test import override_settings
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from openedx_events.learning.data import UserData, UserPersonalData

import edx_event_bus_kafka.publishing.event_producer as ep

try:
    from confluent_kafka import SerializingProducer
except ImportError:
    confluent_kafka = None


class TestEventProducer(TestCase):
    """Test producer."""

    def setUp(self):
        super().setUp()
        ep.get_producer_for_signal.cache_clear()
        ep.get_serializer.cache_clear()

    def test_extract_event_key(self):
        event_data = {
            'user': UserData(
                id=123,
                is_active=True,
                pii=UserPersonalData(
                    username='foobob',
                    email='bob@foo.example',
                    name="Bob Foo",
                )
            )
        }

        assert ep.extract_event_key(event_data, 'user.pii.username') == 'foobob'
        with pytest.raises(Exception,
                           match="Could not extract key from event; lookup in xxx failed at 'xxx' in dictionary"):
            ep.extract_event_key(event_data, 'xxx')
        with pytest.raises(Exception,
                           match="Could not extract key from event; lookup in user.xxx failed at 'xxx' in object"):
            ep.extract_event_key(event_data, 'user.xxx')

    def test_descend_avro_schema(self):
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        schema = AvroSignalSerializer(signal).schema

        assert ep.descend_avro_schema(schema, ['user', 'pii', 'username']) == {"name": "username", "type": "string"}

        with pytest.raises(Exception) as excinfo:
            ep.descend_avro_schema(schema, ['user', 'xxx'])
        assert excinfo.value.args == ("Error traversing Avro schema along path ['user', 'xxx']; failed at 'xxx'.",)
        assert isinstance(excinfo.value.__cause__, IndexError)

    def test_extract_key_schema(self):
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        schema = ep.extract_key_schema(AvroSignalSerializer(signal), 'user.pii.username')
        assert schema == '{"name": "username", "type": "string"}'

    def test_get_producer_for_signal_unconfigured(self):
        """With missing essential settings, just warn and return None."""
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            assert ep.get_producer_for_signal(signal, 'user.id') is None
            assert len(caught_warnings) == 1
            assert str(caught_warnings[0].message).startswith("Cannot configure event-bus-kafka: Missing setting ")

    def test_get_producer_for_signal_configured(self):
        """Creation succeeds when all settings are present."""
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        with override_settings(
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY='some_key',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET='some_secret',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='http://localhost:54321',
                # include these just to maximize code coverage
                EVENT_BUS_KAFKA_API_KEY='some_other_key',
                EVENT_BUS_KAFKA_API_SECRET='some_other_secret',
        ):
            assert isinstance(ep.get_producer_for_signal(signal, 'user.id'), SerializingProducer)

    @patch('edx_event_bus_kafka.publishing.event_producer.logger')
    def test_on_event_deliver(self, mock_logger):
        fake_event = MagicMock()
        fake_event.topic.return_value = 'some_topic'
        fake_event.key.return_value = 'some_key'
        fake_event.partition.return_value = 'some_partition'

        ep.on_event_deliver(Exception("problem!"), fake_event)
        mock_logger.warning.assert_called_once_with("Event delivery failed: Exception('problem!')")

        ep.on_event_deliver(None, fake_event)
        mock_logger.info.assert_called_once_with(
            'Event delivered to topic some_topic; key=some_key; partition=some_partition'
        )

    def test_send_to_event_bus(self):
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        event_data = {
            'user': UserData(
                id=123,
                is_active=True,
                pii=UserPersonalData(
                    username='foobob',
                    email='bob@foo.example',
                    name="Bob Foo",
                )
            )
        }

        mock_producer = MagicMock()
        with patch('edx_event_bus_kafka.publishing.event_producer.get_producer_for_signal', return_value=mock_producer):
            ep.send_to_event_bus(signal, 'user_stuff', 'user.id', event_data)

        mock_producer.produce.assert_called_once_with(
            'user_stuff', key=123, value=event_data,
            on_delivery=ep.on_event_deliver,
            headers={'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'},
        )
