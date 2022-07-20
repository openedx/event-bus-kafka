"""
Tests for event_consumer module.
"""

import copy
from contextlib import contextmanager
from unittest.mock import Mock, patch

from confluent_kafka.serialization import StringSerializer
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.tooling import OpenEdxPublicSignal

import edx_event_bus_kafka.consumer.event_consumer as ec
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


class TestEmitSignals(TestCase):
    """
    Tests for message parsing and signal-sending.
    """

    def setUp(self):
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
        self.normal_message = FakeMessage(
            topic='user_stuff',
            headers=[
                ['ce_type', b'org.openedx.learning.auth.session.login.completed.v1']
            ],
            key=b'\x00\x00\x00\x00\x01\x0cfoobob',  # Avro, as observed in manual test
            value=self.normal_event_data,
            error=None,
        )

    def test_emit(self):
        mock_signal = Mock()
        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type', return_value=mock_signal) as mock_lookup:
            ec.emit_signals_from_message(self.normal_message)

        mock_lookup.assert_called_once_with('org.openedx.learning.auth.session.login.completed.v1')
        mock_signal.send_event.assert_called_once_with(**self.normal_event_data)

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = []

        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type') as mock_lookup:
            ec.emit_signals_from_message(msg)

        mock_lookup.assert_not_called()

    def test_unknown_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = [
            ['ce_type', b'xxxx']
        ]

        with patch.object(OpenEdxPublicSignal, 'get_signal_by_type', side_effect=KeyError('not found')) as mock_lookup:
            # Should just suppress exception and log
            ec.emit_signals_from_message(msg)

        mock_lookup.assert_called_once_with('xxxx')


class TestCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_kafka.consumer.event_consumer.create_consumer')
    def test_kafka_consumers_disabled(self, mock_create_consumer):
        call_command(Command(), topic='test', group_id='test')
        assert not mock_create_consumer.called
