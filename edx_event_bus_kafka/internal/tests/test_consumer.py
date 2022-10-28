"""
Tests for event_consumer module.
"""

import copy
from typing import Optional
from unittest.mock import Mock, call, patch

import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData

from edx_event_bus_kafka.internal.consumer import KafkaEventConsumer, UnusableMessageError
from edx_event_bus_kafka.management.commands.consume_events import Command


class FakeMessage:
    """
    A fake confluent_kafka.cimpl.Message that we can actually construct for mocking.
    """

    def __init__(
            self, topic: str, partition: Optional[int], offset: Optional[int],
            headers: list, key: bytes, value, error
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers = headers
        self._key = key
        self._value = value
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> Optional[int]:
        return self._partition

    def offset(self) -> Optional[int]:
        return self._offset

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
            partition=2,
            offset=12345,
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
        mock_logger.error.assert_called_once_with("Kafka consumers not enabled, exiting.")

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_TOPIC_PREFIX='prod',
    )
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    def test_consume_loop(self, mock_logger):
        """
        Check the basic loop lifecycle.
        """
        emit_call_count = 0

        def fake_emit(*args, **kwargs):
            nonlocal emit_call_count, self
            emit_call_count += 1
            # Perform the following sequence of events on successive calls:
            # 1) accept a message normally, 2) throw, 3) accept a message normally
            # again, and 4) stop the loop so we can finish the unit test.
            if emit_call_count == 1:
                return
            if emit_call_count == 2:
                raise Exception("something broke")
            if emit_call_count == 3:
                return
            else:
                self.event_consumer._shut_down()  # pylint: disable=protected-access

        with patch.object(self.event_consumer, 'emit_signals_from_message', side_effect=fake_emit) as mock_emit:
            mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
            self.event_consumer.consumer = mock_consumer
            self.event_consumer.consume_indefinitely()

        # Check that each of the mocked out methods got called as expected.
        mock_consumer.subscribe.assert_called_once_with(['prod-some-topic'])
        assert mock_emit.call_args_list == [call(self.normal_message)] * 4

        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming event from Kafka: Exception('something broke') in context" in exc_log_msg
        assert "full_topic='prod-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert f"expected_signal={self.mock_signal!r}" in exc_log_msg
        assert "-- event details: " in exc_log_msg
        assert "'partition': 2" in exc_log_msg
        assert "'offset': 12345" in exc_log_msg
        assert "'headers': [['ce_type', b'org.openedx.learning.auth.session.login.completed.v1']]" in exc_log_msg
        assert "'key': b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob'" in exc_log_msg
        assert "email='bob@foo.example'" in exc_log_msg

        mock_consumer.close.assert_called_once_with()  # since shutdown was requested, not because of exception

    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    def test_emit(self, mock_logger):
        self.event_consumer.emit_signals_from_message(self.normal_message)

        mock_logger.error.assert_not_called()
        self.mock_signal.send_event.assert_called_once_with(**self.normal_event_data)

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = []  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Missing ce_type header on message, cannot determine signal",
        )
        assert not self.mock_signal.send_event.called

    def test_unexpected_signal_type_in_header(self):
        msg = copy.copy(self.normal_message)
        msg._headers = [  # pylint: disable=protected-access
            ['ce_type', b'xxxx']
        ]
        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Signal types do not match. Expected org.openedx.learning.auth.session.login.completed.v1. "
            "Received message of type xxxx.",
        )
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
        mock_logger.error.assert_called_once_with("Kafka consumers not enabled, exiting.")
