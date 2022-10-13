"""
Tests for event_consumer module.
"""

import copy
from unittest.mock import Mock, call, patch

import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.tooling import OpenEdxPublicSignal

from edx_event_bus_kafka.internal.consumer import KafkaEventConsumer
from edx_event_bus_kafka.management.commands.consume_events import Command


class FakeMessage:
    """
    A fake confluent_kafka.cimpl.Message that we can actually construct for mocking.
    """

    def __init__(self, topic: str, headers: list, key: bytes, value, error):
        self._topic = topic
        self._headers = headers
        self._key = key
        self._value = value
        self._error = error

    def topic(self) -> str:
        return self._topic

    def headers(self) -> list:
        """List of str/bytes key/value pairs."""
        return self._headers

    def key(self) -> bytes:
        """Bytes (Avro)."""
        return self._key

    def value(self):
        """Deserialized event value."""
        return self._value

    def error(self):
        return self._error


@override_settings(
    EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='https://test-url',
    EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='bootstrap-servers',
    EVENT_BUS_KAFKA_API_KEY='test-key',
    EVENT_BUS_KAFKA_API_SECRET='test-secret',
)
class TestEmitSignals(TestCase):
    """
    Tests for message parsing and signal-sending.
    """

    def setUp(self):
        super().setUp()
        self.normal_event_data = {
            'user': UserData(
                id=123,
                is_active=True,
                pii=UserPersonalData(
                    username='foobob',
                    email='bob@foo.example',
                    name='Bob Foo'
                )
            )
        }
        self.signal_type_bytes = b'org.openedx.learning.auth.session.login.completed.v1'
        self.signal_type = self.signal_type_bytes.decode('utf-8')
        self.normal_message = FakeMessage(
            topic='user_stuff',
            headers=[
                ['ce_type', self.signal_type_bytes]
            ],
            key=b'\x00\x00\x00\x00\x01\x0cfoobob',  # Avro, as observed in manual test
            value=self.normal_event_data,
            error=None,
        )
        self.mock_signal = Mock(event_type=self.signal_type, init_data={})
        self.event_consumer = KafkaEventConsumer('some-topic', 'test_group_id', self.mock_signal)

    @override_settings(EVENT_BUS_KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    def test_consume_loop_disabled(self, mock_logger):
        self.event_consumer.consume_indefinitely()  # returns at all
        mock_logger.error.assert_called_once_with("Kafka consumers not enabled")

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_TOPIC_PREFIX='prod',
    )
    def test_consume_loop(self):
        """
        Check the basic loop lifecycle.
        """
        poll_call_count = 0

        def fake_poll(*args, **kwargs):
            nonlocal poll_call_count
            poll_call_count += 1
            # Return normally twice (to show looping), then throw (to
            # show that we're not actually handling exceptions, at
            # least at the moment.) If we start suppressing
            # exceptions, we'll need some other way to break the loop
            # for this test.
            if poll_call_count >= 3:
                raise Exception("something broke")
            return self.normal_message

        with patch.object(self.event_consumer, 'process_single_message') as mock_process:
            mock_consumer = Mock(**{'poll.side_effect': fake_poll}, autospec=True)
            self.event_consumer.consumer = mock_consumer
            with pytest.raises(Exception, match="something broke"):
                self.event_consumer.consume_indefinitely()

        # Check that each of the mocked out methods got called as expected.
        mock_consumer.subscribe.assert_called_once_with(['prod-some-topic'])
        assert mock_consumer.poll.call_args_list == [
            call(timeout=1.0), call(timeout=1.0), call(timeout=1.0)
        ]
        assert mock_process.call_args_list == [
            call(self.normal_message), call(self.normal_message)
        ]
        mock_consumer.close.assert_called_once_with()

    def test_emit(self):
        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type', return_value=self.mock_signal) as mock_lookup:
            self.event_consumer.emit_signals_from_message(self.normal_message)

        mock_lookup.assert_called_once_with(self.signal_type)
        self.mock_signal.send_event.assert_called_once_with(**self.normal_event_data)

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = []  # pylint: disable=protected-access

        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type') as mock_lookup:
            self.event_consumer.emit_signals_from_message(msg)

        mock_lookup.assert_not_called()

    def test_unknown_type(self):
        # If we pretend that the test signal type is not a real one, behave accordingly.
        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type', side_effect=KeyError('not found')) as mock_lookup:
            # Should just suppress exception and log
            self.event_consumer.emit_signals_from_message(self.normal_message)

        mock_lookup.assert_called_once_with(self.signal_type)
        assert not self.mock_signal.send_event.called

    def test_unwanted_types(self):
        msg = copy.copy(self.normal_message)
        msg._headers = [  # pylint: disable=protected-access
            ['ce_type', b'xxxx']
        ]
        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type', self.mock_signal):
            self.event_consumer.emit_signals_from_message(msg)

        assert not self.mock_signal.send_event.called


class TestCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(EVENT_BUS_KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    def test_kafka_consumers_disabled(self, mock_create_consumer, mock_logger):
        call_command(Command(), topic='test', group_id='test', signal='')
        assert not mock_create_consumer.called
        mock_logger.error.assert_called_once_with("Kafka consumers not enabled")
