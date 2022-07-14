""" Tests for event consumer management command """

from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings

from edx_arch_experiments.kafka_consumer.management.commands.consume_events import Command


# TODO (EventBus): Figure out how to mock messages for more thorough testing
class TestConsumeEventCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_arch_experiments.kafka_consumer.management.commands.consume_events.Command.create_consumer')
    def test_kafka_consumers_disabled(self, mock_create_consumer):
        call_command(Command(), topic='test', group_id='test')
        assert not mock_create_consumer.called

    @patch('edx_arch_experiments.kafka_consumer.management.commands.consume_events.Command.emit_signals_from_message')
    def test_process_single_message_null(self, mock_handle):
        consume_command = Command()
        consume_command.process_single_message(None)
        assert not mock_handle.called
