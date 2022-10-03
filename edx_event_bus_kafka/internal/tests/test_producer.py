"""
Test the event producer code.
"""

import gc
import time
import warnings
from unittest import TestCase
from unittest.mock import Mock, call, patch

import openedx_events.learning.signals
import pytest
from django.test import override_settings
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from openedx_events.learning.data import UserData, UserPersonalData

import edx_event_bus_kafka.internal.producer as ep

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    from confluent_kafka.schema_registry.avro import AvroSerializer
except ImportError:  # pragma: no cover
    pass


class TestEventProducer(TestCase):
    """Test producer."""

    def setUp(self):
        super().setUp()
        self.signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        self.event_data = {
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

    def test_extract_event_key(self):
        assert ep.extract_event_key(self.event_data, 'user.pii.username') == 'foobob'
        with pytest.raises(Exception,
                           match="Could not extract key from event; lookup in xxx failed at 'xxx' in dictionary"):
            ep.extract_event_key(self.event_data, 'xxx')
        with pytest.raises(Exception,
                           match="Could not extract key from event; lookup in user.xxx failed at 'xxx' in object"):
            ep.extract_event_key(self.event_data, 'user.xxx')

    def test_descend_avro_schema(self):
        schema = AvroSignalSerializer(self.signal).schema

        assert ep.descend_avro_schema(schema, ['user', 'pii', 'username']) == {"name": "username", "type": "string"}

        with pytest.raises(Exception) as excinfo:
            ep.descend_avro_schema(schema, ['user', 'xxx'])
        assert excinfo.value.args == ("Error traversing Avro schema along path ['user', 'xxx']; failed at 'xxx'.",)
        assert isinstance(excinfo.value.__cause__, IndexError)

    def test_extract_key_schema(self):
        schema = ep.extract_key_schema(AvroSignalSerializer(self.signal), 'user.pii.username')
        assert schema == '{"name": "username", "type": "string"}'

    def test_serializers_configured(self):
        with override_settings(EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345'):
            key_ser, value_ser = ep.get_serializers(self.signal, 'user.id')
            # We can't actually call them because they want to talk to the schema server.
            assert isinstance(key_ser, AvroSerializer)
            assert isinstance(value_ser, AvroSerializer)

    def test_serializers_unconfigured(self):
        with pytest.raises(Exception, match="missing library or settings"):
            ep.get_serializers(self.signal, 'user.id')

    def test_get_producer_unconfigured(self):
        """With missing essential settings, just warn and return None."""
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            assert ep.get_producer() is None
            assert len(caught_warnings) == 1
            assert str(caught_warnings[0].message).startswith("Cannot configure event-bus-kafka: Missing setting ")

    def test_get_producer_configured(self):
        """Creation succeeds when all settings are present."""
        with override_settings(
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY='some_key',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET='some_secret',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                # include these just to maximize code coverage
                EVENT_BUS_KAFKA_API_KEY='some_other_key',
                EVENT_BUS_KAFKA_API_SECRET='some_other_secret',
        ):
            assert isinstance(ep.get_producer(), ep.KafkaEventProducer)

    @patch('edx_event_bus_kafka.internal.producer.logger')
    def test_on_event_deliver(self, mock_logger):
        fake_event = Mock()
        fake_event.topic.return_value = 'some_topic'
        fake_event.key.return_value = 'some_key'
        fake_event.partition.return_value = 'some_partition'

        ep.on_event_deliver(Exception("problem!"), fake_event)
        mock_logger.warning.assert_called_once_with("Event delivery failed: Exception('problem!')")

        ep.on_event_deliver(None, fake_event)
        mock_logger.info.assert_called_once_with(
            'Event delivered to topic some_topic; key=some_key; partition=some_partition'
        )

    # Mock out the serializers for this one so we don't have to deal
    # with expected Avro bytes -- and they can't call their schema server.
    @patch(
        'edx_event_bus_kafka.internal.producer.get_serializers', autospec=True,
        return_value=(
            lambda _key, _ctx: b'key-bytes-here',
            lambda _value, _ctx: b'value-bytes-here',
        )
    )
    def test_send_to_event_bus(self, mock_get_serializers):
        with override_settings(
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                EVENT_BUS_TOPIC_PREFIX='prod',
        ):
            producer_api = ep.get_producer()
            with patch.object(producer_api, 'producer', autospec=True) as mock_producer:
                producer_api.send(
                    signal=self.signal, topic='user-stuff',
                    event_key_field='user.id', event_data=self.event_data
                )

        mock_get_serializers.assert_called_once_with(self.signal, 'user.id')

        mock_producer.produce.assert_called_once_with(
            'prod-user-stuff', key=b'key-bytes-here', value=b'value-bytes-here',
            on_delivery=ep.on_event_deliver,
            headers={'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'},
        )

    @override_settings(EVENT_BUS_KAFKA_POLL_INTERVAL_SEC=0.05)
    def test_polling_loop_terminates(self):
        """
        Test that polling loop stops as soon as the producer is garbage-collected.
        """
        call_count = 0

        def increment_call_count(*args):
            nonlocal call_count
            call_count += 1

        mock_producer = Mock(**{'poll.side_effect': increment_call_count})
        producer_api = ep.KafkaEventProducer(mock_producer)  # Created, starts polling

        # Allow a little time to pass and check that the mock poll has been called
        time.sleep(1.0)
        assert call_count >= 3  # some small value; would actually be about 20
        print(producer_api)  # Use the value here to ensure it isn't GC'd early

        # Allow garbage collection of these objects, then ask for it to happen.
        producer_api = None
        mock_producer = None
        gc.collect()

        time.sleep(0.2)  # small multiple of loop iteration time
        count_after_gc = call_count

        # Wait a little longer and confirm that the count is no longer rising
        time.sleep(1.0)
        assert call_count == count_after_gc

    @override_settings(EVENT_BUS_KAFKA_POLL_INTERVAL_SEC=0.05)
    def test_polling_loop_robust(self):
        """
        Test that polling loop continues even if one call raises an exception.
        """
        call_count = 0

        def increment_call_count(*args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Exercise error handler on first iteration")

        mock_producer = Mock(**{'poll.side_effect': increment_call_count})
        producer_api = ep.KafkaEventProducer(mock_producer)  # Created, starts polling

        # Allow a little time to pass and check that the mock poll has been called
        time.sleep(1.0)
        assert call_count >= 3  # some small value; would actually be about 20
        print(producer_api)  # Use the value here to ensure it isn't GC'd early

    @override_settings(EVENT_BUS_TOPIC_PREFIX='stage')
    @override_settings(EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345')
    @override_settings(EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321')
    @patch('edx_event_bus_kafka.internal.producer.SerializationContext')
    def test_serialize_and_produce_to_same_topic(self, mock_context):
        producer_api = ep.get_producer()
        with patch('edx_event_bus_kafka.internal.producer.AvroSerializer',
                   return_value=lambda _x, _y: b'bytes-here'):
            with patch.object(producer_api, 'producer', autospec=True) as mock_producer:
                producer_api.send(
                    signal=self.signal, topic='user-stuff',
                    event_key_field='user.id', event_data=self.event_data
                )

        mock_context.assert_has_calls([
            call('stage-user-stuff', 'key', {'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'}),
            call('stage-user-stuff', 'value', {'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'}),
        ])
        assert mock_context.call_count == 2
        mock_producer.produce.assert_called_once_with(
            'stage-user-stuff', key=b'bytes-here', value=b'bytes-here',
            on_delivery=ep.on_event_deliver,
            headers={'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'},
        )
