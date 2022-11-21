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
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED

from edx_event_bus_kafka.internal.consumer import KafkaEventConsumer, ReceiverError, UnusableMessageError
from edx_event_bus_kafka.management.commands.consume_events import Command

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    from confluent_kafka import KafkaError
except ImportError:  # pragma: no cover
    pass


class FakeMessage:
    """
    A fake confluent_kafka.cimpl.Message that we can actually construct for mocking.
    """

    def __init__(
            self, topic: str, partition: Optional[int] = None, offset: Optional[int] = None,
            headers: Optional[list] = None, key: Optional[bytes] = None, value=None,
            error=None,
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

    def headers(self) -> Optional[list]:
        """List of str/bytes key/value pairs."""
        return self._headers

    def key(self) -> Optional[bytes]:
        """Bytes (Avro)."""
        return self._key

    def value(self):
        """Deserialized event value."""
        return self._value

    def error(self):
        return self._error


def fake_receiver_returns_quietly(**kwargs):
    return


def fake_receiver_raises_error(**kwargs):
    raise Exception("receiver whoops")


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
        self.mock_receiver = Mock()
        self.signal = SESSION_LOGIN_COMPLETED
        self.signal.connect(fake_receiver_returns_quietly)
        self.signal.connect(fake_receiver_raises_error)
        self.signal.connect(self.mock_receiver)
        self.event_consumer = KafkaEventConsumer('some-topic', 'test_group_id', self.signal)

    def tearDown(self):
        self.signal.disconnect(fake_receiver_returns_quietly)
        self.signal.disconnect(fake_receiver_raises_error)
        self.signal.disconnect(self.mock_receiver)

    def assert_signal_sent_with(self, signal, data):
        """
        Check that a signal-send came in as expected to the mock receiver.
        """
        self.mock_receiver.assert_called_once()
        call_kwargs = self.mock_receiver.call_args[1]

        # Standard signal stuff
        assert call_kwargs['signal'] == signal
        assert call_kwargs['sender'] is None

        # There should just be one key-value pair in the data for all OpenEdxPublicEvents
        ((event_top_key, event_contents),) = data.items()
        assert call_kwargs[event_top_key] == event_contents

        # There should also be a metadata key -- spot-check it
        metadata = call_kwargs['metadata']
        assert metadata.event_type == signal.event_type
        assert metadata.sourcehost is not None

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
    @patch('edx_event_bus_kafka.internal.consumer.time.sleep', autospec=True)
    def test_consume_loop(self, mock_sleep, mock_logger):
        """
        Check the basic loop lifecycle.
        """
        def raise_exception():
            raise Exception("something broke")

        # How the emit_signals_from_message() mock will behave on each successive call.
        mock_emit_side_effects = [
            lambda: None,  # accept and ignore a message
            raise_exception,
            lambda: None,  # accept another message (exception didn't break loop)

            # Final "call" just serves to stop the loop
            self.event_consumer._shut_down  # pylint: disable=protected-access
        ]
        next_emit_behavior = 0  # index into the above

        def fake_emit(*args, **kwargs):
            """
            Call each function in mock_emit_side_effects() on successive invocations.
            """
            nonlocal mock_emit_side_effects, next_emit_behavior
            to_run = mock_emit_side_effects[next_emit_behavior]
            next_emit_behavior += 1
            return to_run()

        with patch.object(self.event_consumer, 'emit_signals_from_message', side_effect=fake_emit) as mock_emit:
            mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
            self.event_consumer.consumer = mock_consumer
            self.event_consumer.consume_indefinitely()

        # Check that each of the mocked out methods got called as expected.
        mock_consumer.subscribe.assert_called_once_with(['prod-some-topic'])
        # Check that emit was called the expected number of times
        assert mock_emit.call_args_list == [call(self.normal_message)] * len(mock_emit_side_effects)

        # Check that there was one error log message and that it contained all the right parts,
        # in some order.
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming event from Kafka: Exception('something broke') in context" in exc_log_msg
        assert "full_topic='prod-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert ("expected_signal=<OpenEdxPublicSignal: "
                "org.openedx.learning.auth.session.login.completed.v1>") in exc_log_msg
        assert "-- event details: " in exc_log_msg
        assert "'partition': 2" in exc_log_msg
        assert "'offset': 12345" in exc_log_msg
        assert "'headers': [['ce_type', b'org.openedx.learning.auth.session.login.completed.v1']]" in exc_log_msg
        assert "'key': b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob'" in exc_log_msg
        assert "email='bob@foo.example'" in exc_log_msg

        # Check that each message got committed (including the errored ones)
        assert len(mock_consumer.commit.call_args_list) == len(mock_emit_side_effects)

        mock_sleep.assert_not_called()
        mock_consumer.close.assert_called_once_with()  # since shutdown was requested, not because of exception

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_TOPIC_PREFIX='prod',
        EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP=1
    )
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.time.sleep', autospec=True)
    def test_record_error_no_event(self, mock_sleep, mock_logger):
        """
        Covers reporting of an error in the consumer loop when no event is available
        for logging. (Event-present is already covered in consume_loop test.)
        """
        def poll_side_effect(*args, **kwargs):
            # Only run one iteration
            self.event_consumer._shut_down()  # pylint: disable=protected-access
            raise Exception("something random")

        mock_consumer = Mock(**{'poll.side_effect': poll_side_effect}, autospec=True)
        self.event_consumer.consumer = mock_consumer
        self.event_consumer.consume_indefinitely()

        # Check that there was one error log message and that it contained all the right parts,
        # in some order.
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming event from Kafka: Exception('something random') in context" in exc_log_msg
        assert "full_topic='prod-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert ("expected_signal=<OpenEdxPublicSignal: "
                "org.openedx.learning.auth.session.login.completed.v1>") in exc_log_msg
        assert "-- no event available" in exc_log_msg

        # No-event sleep branch was triggered
        mock_sleep.assert_called_once_with(1)

        mock_consumer.commit.assert_not_called()

    def test_check_event_error(self):
        """
        DeserializingConsumer.poll() should never return a Message with an error() object,
        but we check it anyway as a safeguard. This test exercises that branch.
        """
        with pytest.raises(Exception) as exc_info:
            self.event_consumer.emit_signals_from_message(
                FakeMessage(
                    topic='user_stuff',
                    partition=2,
                    error=KafkaError(123, "done broke"),
                )
            )

        assert exc_info.value.args == (
            "Polled message had error object (shouldn't happen): "
            "KafkaError{code=ERR_123?,val=123,str=\"done broke\"}",
        )

    @patch('django.dispatch.dispatcher.logger', autospec=True)
    def test_emit(self, mock_logger):
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer.emit_signals_from_message(self.normal_message)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        assert exc_info.value.args == (
            "1 receiver(s) out of 3 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "
            "edx_event_bus_kafka.internal.tests.test_consumer.fake_receiver_raises_error="
            "Exception('receiver whoops')",
        )

        # Check that django dispatch is logging the stack trace. Really, we only care that
        # *something* does it, though. This test just ensures that our "(stack trace
        # elsewhere in logs)" isn't a lie.
        (receiver_error,) = exc_info.value.causes
        mock_logger.error.assert_called_once_with(
            "Error calling %s in Signal.send_robust() (%s)",
            'fake_receiver_raises_error',
            receiver_error,
            exc_info=receiver_error,
        )

    def test_malformed_receiver_errors(self):
        """
        Ensure that even a really messed-up receiver is still reported correctly.
        """
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer._check_receiver_results([  # pylint: disable=protected-access
                (lambda x:x, Exception("for lambda")),
                # This would actually raise an error inside send_robust(), but it will serve well enough for testing...
                ("not even a function", Exception("just plain bad")),
            ])
        assert exc_info.value.args == (
            "2 receiver(s) out of 2 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "

            "edx_event_bus_kafka.internal.tests.test_consumer.TestEmitSignals."
            "test_malformed_receiver_errors.<locals>.<lambda>=Exception('for lambda'), "

            "not even a function=Exception('just plain bad')",
        )

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        msg._headers = []  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Missing ce_type header on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

    def test_multiple_types(self):
        """
        Very unlikely case, but this gets us coverage.
        """
        msg = copy.copy(self.normal_message)
        msg._headers = [['ce_type', b'abc'], ['ce_type', b'def']]  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg)

        assert excinfo.value.args == (
            "Multiple ce_type headers found on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

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
        assert not self.mock_receiver.called

    def test_no_commit_if_no_error_logged(self):
        """
        Check that if there is an error that we fail to log, we do not commit the offset
        """
        def raise_exception(*args, **kwargs):
            raise Exception("something broke")

        with patch.object(self.event_consumer, 'emit_signals_from_message', side_effect=raise_exception):
            with patch.object(self.event_consumer, 'record_event_consuming_error', side_effect=raise_exception):
                with pytest.raises(Exception):
                    mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
                    self.event_consumer.consumer = mock_consumer
                    self.event_consumer.consume_indefinitely()

        mock_consumer.commit.assert_not_called()


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
