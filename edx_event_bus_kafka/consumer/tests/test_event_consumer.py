"""
Tests for event_consumer module.
"""

from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings

import edx_event_bus_kafka.consumer.event_consumer as ec
from edx_event_bus_kafka.management.commands.consume_events import Command


class TestConsumer(TestCase):
    """
    Tests for the core consumer functionality.
    """


# TODO (EventBus): Figure out how to mock messages for more thorough testing
class TestCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_kafka.consumer.event_consumer.create_consumer')
    def test_kafka_consumers_disabled(self, mock_create_consumer):
        call_command(Command(), topic='test', group_id='test')
        assert not mock_create_consumer.called

    @patch('edx_event_bus_kafka.consumer.event_consumer.emit_signals_from_message')
    def test_process_single_message_null(self, mock_handle):
        ec.process_single_message(None)
        assert not mock_handle.called
