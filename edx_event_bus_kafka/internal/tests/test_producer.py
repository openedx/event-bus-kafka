"""
Test the event producer code.
"""

import gc
import time
import warnings
from unittest import TestCase
from unittest.mock import ANY, Mock, call, patch

import openedx_events.event_bus
import openedx_events.learning.signals
import pytest
from django.test import override_settings
from openedx_events.data import EventsMetadata
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer
from openedx_events.event_bus.avro.tests.test_utilities import SubTestData0, create_simple_signal
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

    def test_create_producer_unconfigured(self):
        """With missing essential settings, just warn and return None."""
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            assert ep.create_producer() is None
            assert len(caught_warnings) == 1
            assert str(caught_warnings[0].message).startswith("Cannot configure event-bus-kafka: Missing setting ")

    def test_create_producer_configured(self):
        """
        Creation succeeds when all settings are present.

        Also tests basic compliance with the implementation-loader API in openedx-events.
        """
        with override_settings(
                EVENT_BUS_PRODUCER='edx_event_bus_kafka.create_producer',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_KEY='some_key',
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_API_SECRET='some_secret',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                # include these just to maximize code coverage
                EVENT_BUS_KAFKA_API_KEY='some_other_key',
                EVENT_BUS_KAFKA_API_SECRET='some_other_secret',
        ):
            assert isinstance(openedx_events.event_bus.get_producer(), ep.KafkaEventProducer)

    @patch('edx_event_bus_kafka.internal.producer.logger')
    def test_on_event_deliver(self, mock_logger):
        fake_event = Mock()
        fake_event.topic.return_value = 'some_topic'
        fake_event.key.return_value = 'some_key'
        fake_event.partition.return_value = 'some_partition'

        # simple producing context, we check the full object in other tests
        context = ep.ProducingContext(full_topic='some_topic')

        # ensure on_event_deliver reports the entire calling context if there was an error
        context.on_event_deliver(Exception("problem!"), fake_event)

        # extract the error message that was produced and check it has all relevant information (order isn't guaranteed
        # and doesn't actually matter, nor do we want to worry if other information is added later)
        (error_string,) = mock_logger.exception.call_args.args
        assert "full_topic='some_topic'" in error_string
        assert "error=problem!" in error_string

        context.on_event_deliver(None, fake_event)
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
                SERVICE_VARIANT='test',
        ):
            metadata = EventsMetadata(event_type=self.signal.event_type,
                                      minorversion=0)
            producer_api = ep.create_producer()
            with patch.object(producer_api, 'producer', autospec=True) as mock_producer:
                producer_api.send(
                    signal=self.signal, topic='user-stuff',
                    event_key_field='user.id', event_data=self.event_data, event_metadata=metadata
                )

        mock_get_serializers.assert_called_once_with(self.signal, 'user.id')

        expected_headers = {
                'ce_type': b'org.openedx.learning.auth.session.login.completed.v1',
                'ce_id': str(metadata.id).encode("utf8"),
                'ce_source': b'openedx/test/web',
                'sourcehost': metadata.sourcehost.encode("utf8"),
                'ce_specversion': b'1.0',
                'content-type': b'application/avro',
                'ce_datacontenttype': b'application/avro'
            }

        mock_producer.produce.assert_called_once_with(
            'prod-user-stuff', key=b'key-bytes-here', value=b'value-bytes-here', on_delivery=ANY,
            headers=expected_headers,
        )

    @patch(
        'edx_event_bus_kafka.internal.producer.get_serializers', autospec=True,
        return_value=(
            lambda _key, _ctx: b'key-bytes-here',
            lambda _value, _ctx: b'value-bytes-here',
        )
    )
    @patch('edx_event_bus_kafka.internal.producer.logger')
    def test_full_event_data_present_in_key_extraction_error(self, mock_logger, *args):
        simple_signal = create_simple_signal({'test_data': SubTestData0})
        with override_settings(
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                EVENT_BUS_TOPIC_PREFIX='dev',
                SERVICE_VARIANT='test',
        ):
            metadata = EventsMetadata(event_type=simple_signal.event_type, minorversion=0)
            producer_api = ep.create_producer()
            # force an exception with a bad event_key_field
            producer_api.send(signal=simple_signal, topic='topic', event_key_field='bad_field',
                              event_data={'test_data': SubTestData0(sub_name="name", course_id="id")},
                              event_metadata=metadata)

        (error_string,) = mock_logger.exception.call_args.args
        assert "event_data={'test_data': SubTestData0(sub_name='name', course_id='id')}" in error_string
        assert "signal=<OpenEdxPublicSignal: simple.signal>" in error_string
        assert "initial_topic='topic'" in error_string
        assert "full_topic='dev-topic'" in error_string
        assert "event_key_field='bad_field'" in error_string
        assert "event_type='simple.signal'" in error_string
        assert "source='openedx/test/web'" in error_string
        assert f"id=UUID('{metadata.id}')" in error_string
        assert f"sourcehost='{metadata.sourcehost}'" in error_string

    @patch(
        'edx_event_bus_kafka.internal.producer.get_serializers', autospec=True,
        return_value=(
            lambda _key, _ctx: b'key-bytes-here',
            lambda _value, _ctx: b'value-bytes-here',
        )
    )
    @patch('edx_event_bus_kafka.internal.producer.logger')
    def test_full_event_data_present_in_kafka_error(self, mock_logger, *args):
        simple_signal = create_simple_signal({'test_data': SubTestData0})
        with override_settings(
                EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
                EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
                EVENT_BUS_TOPIC_PREFIX='dev',
                SERVICE_VARIANT='test',
        ):
            producer_api = ep.create_producer()
            metadata = EventsMetadata(event_type=simple_signal.event_type, minorversion=0)
            with patch.object(producer_api, 'producer', autospec=True) as mock_producer:
                # imitate a failed send to Kafka
                mock_producer.produce = Mock(side_effect=Exception('bad!'))
                producer_api.send(signal=simple_signal, topic='topic', event_key_field='test_data.course_id',
                                  event_data={'test_data': SubTestData0(sub_name="name", course_id="ABCx")},
                                  event_metadata=metadata)

        (error_string,) = mock_logger.exception.call_args.args
        assert "event_data={'test_data': SubTestData0(sub_name='name', course_id='ABCx')}" in error_string
        assert "signal=<OpenEdxPublicSignal: simple.signal>" in error_string
        assert "initial_topic='topic'" in error_string
        assert "full_topic='dev-topic'" in error_string
        assert "event_key_field='test_data.course_id'" in error_string
        assert "source='openedx/test/web'" in error_string
        assert f"id=UUID('{metadata.id}')" in error_string
        assert f"sourcehost='{metadata.sourcehost}'" in error_string
        # since we didn't fail until after key extraction we should have an event_key to report
        assert "event_key='ABCx'" in error_string
        assert "error=bad!" in error_string

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
        producer_api = ep.create_producer()
        with patch('edx_event_bus_kafka.internal.producer.AvroSerializer',
                   return_value=lambda _x, _y: b'bytes-here'):
            with patch.object(producer_api, 'producer', autospec=True) as mock_producer:
                producer_api.send(
                    signal=self.signal, topic='user-stuff',
                    event_key_field='user.id', event_data=self.event_data,
                    event_metadata=EventsMetadata(event_type=self.signal.event_type, minorversion=0)
                )

        mock_context.assert_has_calls([
            call('stage-user-stuff', 'key', ANY),
            call('stage-user-stuff', 'value', ANY),
        ])
        assert mock_context.call_count == 2
        mock_producer.produce.assert_called_once_with(
            'stage-user-stuff', key=b'bytes-here', value=b'bytes-here',
            on_delivery=ANY,
            # headers are tested elsewhere, we just want to verify the topics
            headers=ANY,
        )

    def test_headers_from_event_metadata(self):
        with override_settings(SERVICE_VARIANT='test'):
            metadata = EventsMetadata(event_type=self.signal.event_type, minorversion=0)
            headers = ep._get_headers_from_metadata(event_metadata=metadata)  # pylint: disable=protected-access
            self.assertDictEqual(headers, {
                'ce_type': b'org.openedx.learning.auth.session.login.completed.v1',
                'ce_id': str(metadata.id).encode("utf8"),
                'ce_source': b'openedx/test/web',
                'ce_specversion': b'1.0',
                'sourcehost': metadata.sourcehost.encode("utf8"),
                'content-type': b'application/avro',
                'ce_datacontenttype': b'application/avro',
            })
