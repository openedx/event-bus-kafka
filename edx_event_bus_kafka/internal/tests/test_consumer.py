"""
Tests for event_consumer module.
"""

import copy
from datetime import datetime
from unittest.mock import Mock, call, patch
from uuid import uuid1

import ddt
import pytest
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from openedx_events.learning.data import UserData, UserPersonalData
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED
from openedx_events.tooling import OpenEdxPublicSignal

from edx_event_bus_kafka.internal.consumer import (
    KafkaEventConsumer,
    ReceiverError,
    UnusableMessageError,
    get_deserializer,
)
from edx_event_bus_kafka.internal.tests.test_utils import FakeMessage, side_effects
from edx_event_bus_kafka.management.commands.consume_events import Command

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    from confluent_kafka import (
        TIMESTAMP_CREATE_TIME,
        TIMESTAMP_NOT_AVAILABLE,
        KafkaError,
        KafkaException,
        TopicPartition,
    )
    from confluent_kafka.error import ConsumeError
except ImportError as e:  # pragma: no cover
    pass


def fake_receiver_returns_quietly(**kwargs):
    return


def fake_receiver_raises_error(**kwargs):
    raise Exception("receiver whoops")


@override_settings(
    EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='https://test-url',
    EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='bootstrap-servers',
    EVENT_BUS_KAFKA_API_KEY='test-key',
    EVENT_BUS_KAFKA_API_SECRET='test-secret',
    EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=10,  # prevent infinite looping if tests are broken
)
@ddt.ddt
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
        # determined by manual testing
        self.normal_event_data_bytes = b'\x00\x00\x01\x86\xb3\xf6\x01\x01\x0cfoobob\x1ebob@foo.example\x0eBob Foo'

        self.message_id = uuid1()
        self.message_id_bytes = str(self.message_id).encode('utf-8')

        self.signal_type_bytes = b'org.openedx.learning.auth.session.login.completed.v1'
        self.signal_type = self.signal_type_bytes.decode('utf-8')
        self.normal_message = FakeMessage(
            topic='local-some-topic',
            partition=2,
            offset=12345,
            headers=[
                ('ce_id', self.message_id_bytes),
                ('ce_type', self.signal_type_bytes),
            ],
            key=b'\x00\x00\x00\x00\x01\x0cfoobob',  # Avro, as observed in manual test
            value=self.normal_event_data_bytes,
            error=None,
            timestamp=(TIMESTAMP_CREATE_TIME, 1675114920123),
        )
        self.mock_receiver = Mock()
        self.signal = SESSION_LOGIN_COMPLETED
        self.signal.connect(fake_receiver_returns_quietly)
        self.signal.connect(fake_receiver_raises_error)
        self.signal.connect(self.mock_receiver)
        self.event_consumer = KafkaEventConsumer('some-topic', 'test_group_id')

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

    def test_reset_offsets_and_sleep_indefinitely(self):
        test_time = datetime.now()
        self.event_consumer.consumer = Mock()
        self.event_consumer._shut_down()  # pylint: disable=protected-access
        self.event_consumer.reset_offsets_and_sleep_indefinitely(offset_timestamp=test_time)
        reset_offsets = self.event_consumer.consumer.subscribe.call_args[1]['on_assign']
        partitions = [TopicPartition('dummy_topic', 0, 0), TopicPartition('dummy_topic', 1, 0)]
        self.event_consumer.consumer.offsets_for_times.return_value = partitions

        # This is usually called by Kafka after assignment of partitions. For testing, we are calling
        # it directly.
        reset_offsets(self.event_consumer.consumer, partitions)

        test_time_ms = int(test_time.timestamp()*1000)

        # TopicPartition objects are considered equal if the topic and partition are equal, regardless of offset
        # so we need to compare the objects by property
        [partition_0, partition_1] = self.event_consumer.consumer.offsets_for_times.call_args.args[0]
        assert partition_0.offset == test_time_ms
        assert partition_0.topic == 'dummy_topic'
        assert partition_0.partition == 0

        assert partition_1.offset == test_time_ms
        assert partition_1.topic == 'dummy_topic'
        assert partition_1.partition == 1

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_TOPIC_PREFIX='local',
    )
    @patch('edx_event_bus_kafka.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.time.sleep', autospec=True)
    def test_consume_loop(self, mock_sleep, mock_logger, mock_set_custom_attribute):
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

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects(mock_emit_side_effects),
        ) as mock_emit:
            with patch('edx_event_bus_kafka.internal.consumer.AvroDeserializer',
                       return_value=lambda _x, _y: self.normal_event_data):
                mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
                self.event_consumer.consumer = mock_consumer
                self.event_consumer.consume_indefinitely()

        # Check that each of the mocked out methods got called as expected.
        mock_consumer.subscribe.assert_called_once_with(['local-some-topic'])
        # Check that emit was called the expected number of times
        assert mock_emit.call_args_list == [call(self.normal_message, self.signal)] * len(mock_emit_side_effects)

        # Check that there was one error log message and that it contained all the right parts,
        # in some order.
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert "Error consuming event from Kafka: Exception('something broke') in context" in exc_log_msg
        assert "full_topic='local-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        assert "-- event details: " in exc_log_msg
        assert "'partition': 2" in exc_log_msg
        assert "'offset': 12345" in exc_log_msg
        assert f"'headers': [('ce_id', b'{self.message_id}'), " \
               "('ce_type', b'org.openedx.learning.auth.session.login.completed.v1')]" in exc_log_msg
        assert "'key': b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob'" in exc_log_msg
        assert "email='bob@foo.example'" in exc_log_msg

        mock_set_custom_attribute.assert_has_calls(
            [
                call("kafka_topic", "local-some-topic"),
                call("kafka_message_id", str(self.message_id)),
                call("kafka_partition", 2),
                call("kafka_offset", 12345),
                call("kafka_event_type", "org.openedx.learning.auth.session.login.completed.v1"),
            ] * len(mock_emit_side_effects),
            any_order=True,
        )

        # Check that each message got committed (including the errored ones)
        assert len(mock_consumer.commit.call_args_list) == len(mock_emit_side_effects)

        mock_sleep.assert_not_called()
        mock_consumer.close.assert_called_once_with()  # since shutdown was requested, not because of exception

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=4,
    )
    def test_consecutive_error_limit(self):
        """Confirm that consecutive errors can break out of loop."""
        def raise_exception():
            raise Exception("something broke")

        exception_count = 4

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects([raise_exception] * exception_count)
        ) as mock_emit:
            with patch('edx_event_bus_kafka.internal.consumer.AvroDeserializer',
                       return_value=lambda _x, _y: self.normal_event_data):
                mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
                self.event_consumer.consumer = mock_consumer
                with pytest.raises(Exception) as exc_info:
                    self.event_consumer.consume_indefinitely()

        assert mock_emit.call_args_list == [call(self.normal_message, self.signal)] * exception_count
        assert exc_info.value.args == ("Too many consecutive errors, exiting (4 in a row)",)

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
    )
    @patch('edx_event_bus_kafka.internal.consumer.connection')
    @ddt.data(
        (False, False, False),  # no connection, don't reconnect
        (True, False, True),  # connection unusable, reconnect expected
        (False, True, False),  # usable connection, no need to reconnect
    )
    @ddt.unpack
    def test_connection_reset(self, has_connection, is_usable, reconnect_expected, mock_connection):
        """Confirm we reconnect to the database as required"""
        if not has_connection:
            mock_connection.connection = None
        mock_connection.is_usable.return_value = is_usable

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects([self.event_consumer._shut_down])  # pylint: disable=protected-access
        ):
            with patch('edx_event_bus_kafka.internal.consumer.AvroDeserializer',
                       return_value=lambda _x, _y: self.normal_event_data):
                mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
                self.event_consumer.consumer = mock_consumer
                self.event_consumer.consume_indefinitely()

        if reconnect_expected:
            mock_connection.connect.assert_called_once()
        else:
            mock_connection.connect.assert_not_called()

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=4,
    )
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.time.sleep', autospec=True)
    def test_no_consume_with_offsets(self, mock_sleep, mock_logger):
        mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
        self.event_consumer.consumer = mock_consumer
        mock_sleep.side_effect = side_effects([self.event_consumer._shut_down])  # pylint: disable=protected-access
        self.event_consumer.consume_indefinitely(offset_timestamp=0)
        mock_sleep.assert_called_with(30)
        mock_logger.info.assert_any_call('Offsets are being reset. Sleeping instead of consuming events.')

    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT=4,
    )
    def test_non_consecutive_errors(self):
        """Confirm that non-consecutive errors may not break out of loop."""
        def raise_exception():
            raise Exception("something broke")

        mock_emit_side_effects = [
            raise_exception, raise_exception,
            lambda: None,  # an iteration that doesn't raise an exception
            raise_exception, raise_exception,
            # Stop the loop, since the non-consecutive exceptions won't do it
            self.event_consumer._shut_down,  # pylint: disable=protected-access
        ]

        with patch.object(
                self.event_consumer, 'emit_signals_from_message',
                side_effect=side_effects(mock_emit_side_effects)
        ) as mock_emit:
            with patch('edx_event_bus_kafka.internal.consumer.AvroDeserializer',
                       return_value=lambda _x, _y: self.normal_event_data):
                mock_consumer = Mock(**{'poll.return_value': self.normal_message}, autospec=True)
                self.event_consumer.consumer = mock_consumer
                self.event_consumer.consume_indefinitely()  # exits normally

        assert mock_emit.call_args_list == [call(self.normal_message, self.signal)] * len(mock_emit_side_effects)

    TEST_FAILED_MESSAGE = FakeMessage(
        partition=7,
        offset=6789,
        headers=[
            ('ce_id', b'1111-1111'),
            ('ce_type', b'org.openedx.learning.auth.session.login.completed.v1'),
        ],
        key=b'\x00\x00\x00\x00\x01\x0cfoobob',  # Avro, as observed in manual test
        value=b'XXX',
    )
    TEST_KAFKA_ERROR = KafkaError(2, fatal=False, retriable=True)
    TEST_CONSUME_ERROR_NO_MESSAGE = ConsumeError(TEST_KAFKA_ERROR, exception=None, kafka_message=None)
    TEST_CONSUME_ERROR_WITH_MESSAGE = ConsumeError(TEST_KAFKA_ERROR, exception=None, kafka_message=TEST_FAILED_MESSAGE)
    TEST_KAFKA_EXCEPTION = KafkaException(TEST_KAFKA_ERROR)
    TEST_KAFKA_FATAL_ERROR = KafkaError(2, fatal=True, retriable=True)
    TEST_CONSUME_ERROR_FATAL = ConsumeError(TEST_KAFKA_FATAL_ERROR, exception=None, kafka_message=None)

    @patch('edx_event_bus_kafka.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.time.sleep', autospec=True)
    @override_settings(
        EVENT_BUS_KAFKA_SCHEMA_REGISTRY_URL='http://localhost:12345',
        EVENT_BUS_KAFKA_BOOTSTRAP_SERVERS='localhost:54321',
        EVENT_BUS_TOPIC_PREFIX='local',
        EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP=1
    )
    @ddt.data(
        (Exception("something random"), False, False, False),
        (TEST_CONSUME_ERROR_NO_MESSAGE, False, True, False),
        (TEST_CONSUME_ERROR_WITH_MESSAGE, True, True, False),
        (TEST_KAFKA_EXCEPTION, False, True, False),
        (TEST_CONSUME_ERROR_FATAL, False, True, True),
    )
    @ddt.unpack
    def test_record_error_for_various_errors(
        self, exception, has_message, has_kafka_error, is_fatal, mock_sleep, mock_logger, mock_set_custom_attribute,
    ):
        """
        Covers reporting of an error in the consumer loop for various types of errors.
        """
        def poll_side_effect(*args, **kwargs):
            # Only run one iteration
            self.event_consumer._shut_down()  # pylint: disable=protected-access
            raise exception

        mock_consumer = Mock(**{'poll.side_effect': poll_side_effect}, autospec=True)
        self.event_consumer.consumer = mock_consumer
        if is_fatal:
            with pytest.raises(ConsumeError) as exc_info:
                self.event_consumer.consume_indefinitely()
            assert exc_info.value == exception
        else:
            self.event_consumer.consume_indefinitely()

        # Check that there was one exception log message and that it contained all the right parts,
        # in some order.
        mock_logger.error.assert_not_called()
        mock_logger.exception.assert_called_once()
        (exc_log_msg,) = mock_logger.exception.call_args.args
        assert f"Error consuming event from Kafka: {repr(exception)} in context" in exc_log_msg
        assert "full_topic='local-some-topic'" in exc_log_msg
        assert "consumer_group='test_group_id'" in exc_log_msg
        if has_message:
            assert "-- event details" in exc_log_msg
        else:
            assert "-- no event available" in exc_log_msg

        expected_custom_attribute_calls = [
            call("kafka_topic", "local-some-topic"),
        ]
        if has_message:
            expected_custom_attribute_calls += [
                call("kafka_message_id", "1111-1111"),
                call("kafka_event_type", "org.openedx.learning.auth.session.login.completed.v1"),
            ]
        if has_kafka_error:
            expected_custom_attribute_calls += [
                call('kafka_error_fatal', is_fatal),
                call('kafka_error_retriable', True),
            ]
        mock_set_custom_attribute.assert_has_calls(expected_custom_attribute_calls, any_order=True)

        # For non-fatal errors, "no-event" sleep branch was triggered
        if not is_fatal:
            mock_sleep.assert_called_once_with(1)

        mock_consumer.commit.assert_not_called()

    def test_check_event_error(self):
        with pytest.raises(Exception) as exc_info:
            self.event_consumer.emit_signals_from_message(
                FakeMessage(
                    partition=2,
                    error=KafkaError(123, "done broke"),
                    headers=[{'ce_type': 'org.openedx.learning.auth.session.login.completed.v1'}]
                ),
                self.signal
            )

        assert exc_info.value.args == (
            "Polled message had error object: "
            "KafkaError{code=ERR_123?,val=123,str=\"done broke\"}",
        )

    @override_settings(
        EVENT_BUS_TOPIC_PREFIX='local',
    )
    @patch('edx_event_bus_kafka.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @ddt.data(True, False)
    def test_emit_success(self, audit_logging, mock_logger, mock_set_attribute):
        self.signal.disconnect(fake_receiver_raises_error)  # just successes for this one!
        # assume we've already deserialized the data
        self.normal_message.set_value(self.normal_event_data)

        with override_settings(EVENT_BUS_KAFKA_AUDIT_LOGGING_ENABLED=audit_logging):
            self.event_consumer.emit_signals_from_message(self.normal_message, self.signal)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        # Specifically, not called with 'kafka_logging_error'
        mock_set_attribute.assert_not_called()
        if audit_logging:
            mock_logger.info.assert_has_calls([
                call(
                    "Message received from Kafka: topic=local-some-topic, partition=2, "
                    f"offset=12345, message_id={self.message_id}, "
                    "key=b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob', event_timestamp_ms=1675114920123"
                ),
                call('Message from Kafka processed successfully'),
            ])
        else:
            mock_logger.info.assert_not_called()

    @override_settings(
        EVENT_BUS_TOPIC_PREFIX='local',
    )
    @patch('edx_event_bus_kafka.internal.consumer.set_custom_attribute', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    def test_emit_success_tolerates_missing_timestamp(self, mock_logger, mock_set_attribute):
        self.signal.disconnect(fake_receiver_raises_error)  # just successes for this one!
        # assume we've already deserialized the data
        self.normal_message.set_value(self.normal_event_data)
        self.normal_message._timestamp = (TIMESTAMP_NOT_AVAILABLE, None)  # pylint: disable=protected-access

        self.event_consumer.emit_signals_from_message(self.normal_message, self.signal)
        self.assert_signal_sent_with(self.signal, self.normal_event_data)
        # Specifically, not called with 'kafka_logging_error'
        mock_set_attribute.assert_not_called()
        mock_logger.info.assert_has_calls([
            call(
                "Message received from Kafka: topic=local-some-topic, partition=2, "
                f"offset=12345, message_id={self.message_id}, "
                "key=b'\\x00\\x00\\x00\\x00\\x01\\x0cfoobob', event_timestamp_ms=none"
            ),
            call('Message from Kafka processed successfully'),
        ])

    @patch('django.dispatch.dispatcher.logger', autospec=True)
    def test_emit(self, mock_logger):
        # assume we've already deserialized the data
        self.normal_message.set_value(self.normal_event_data)
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer.emit_signals_from_message(self.normal_message, self.signal)
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

    def test_emit_type_mismatch(self):
        # assume we've already deserialized the data
        self.normal_message.set_value(self.normal_event_data)
        self.normal_message._headers = [('ce_type', b'xxxx')]  # pylint: disable=protected-access

        with pytest.raises(Exception) as excinfo:
            self.event_consumer.emit_signals_from_message(self.normal_message, self.signal)

        assert excinfo.value.args == (
            "Error emitting event from Kafka: (UNEXPECTED) message event type xxxx "
            "does not match signal org.openedx.learning.auth.session.login.completed.v1",
        )
        assert not self.mock_receiver.called

    def test_deserialize_type_mismatch(self):
        self.normal_message._headers = [('ce_type', b'xxxx')]  # pylint: disable=protected-access

        with pytest.raises(Exception) as excinfo:
            self.event_consumer._deserialize_message_value(  # pylint: disable=protected-access
                                                           self.normal_message, self.signal
                                                          )

        assert excinfo.value.args == (
            "Error deserializing event from Kafka: (UNEXPECTED) message event type xxxx "
            "does not match signal org.openedx.learning.auth.session.login.completed.v1",
        )
        assert not self.mock_receiver.called

    def test_malformed_receiver_errors(self):
        """
        Ensure that even a really messed-up receiver is still reported correctly.
        """
        with pytest.raises(ReceiverError) as exc_info:
            self.event_consumer._check_receiver_results([  # pylint: disable=protected-access
                (lambda x:x, Exception("for lambda")),
                # This would actually raise an error inside send_robust(), but it will serve well enough for testing...
                ("not even a function", Exception("just plain bad")),
            ], self.signal)
        assert exc_info.value.args == (
            "2 receiver(s) out of 2 produced errors (stack trace elsewhere in logs) "
            "when handling signal <OpenEdxPublicSignal: "
            "org.openedx.learning.auth.session.login.completed.v1>: "

            "edx_event_bus_kafka.internal.tests.test_consumer.TestEmitSignals."
            "test_malformed_receiver_errors.<locals>.<lambda>=Exception('for lambda'), "

            "not even a function=Exception('just plain bad')",
        )

    def test_determine_signal_success(self):
        determined_signal = self.event_consumer.determine_signal(self.normal_message)
        assert isinstance(determined_signal, OpenEdxPublicSignal)
        assert determined_signal.event_type == self.signal_type

    def test_no_type(self):
        msg = copy.copy(self.normal_message)
        # assume we've already deserialized the data
        msg.set_value(self.normal_event_data)
        msg._headers = []  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.determine_signal(msg)

        assert excinfo.value.args == (
            "Missing ce_type header on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

    def test_multiple_types(self):
        """
        Very unlikely case, but this gets us coverage.
        """
        msg = copy.copy(self.normal_message)
        # assume we've already deserialized the data
        msg.set_value(self.normal_event_data)
        msg._headers = [['ce_type', b'abc'], ['ce_type', b'def']]  # pylint: disable=protected-access

        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.determine_signal(msg)

        assert excinfo.value.args == (
            "Multiple ce_type headers found on message, cannot determine signal",
        )
        assert not self.mock_receiver.called

    def test_unexpected_signal_type_in_header(self):
        msg = copy.copy(self.normal_message)
        # assume we've already deserialized the data
        msg.set_value(self.normal_event_data)
        msg._headers = [  # pylint: disable=protected-access
            ['ce_type', b'xxxx']
        ]
        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.determine_signal(msg)

        assert excinfo.value.args == (
            "Unrecognized type xxxx found on message, cannot determine signal",
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

    def test_bad_headers(self):
        """
        Check that if we cannot process the message headers, we raise an UnusableMessageError

        The various kinds of bad headers are more fully tested in test_utils
        """
        msg = copy.copy(self.normal_message)
        # assume we've already deserialized the data
        msg.set_value(self.normal_event_data)
        msg._headers = [  # pylint: disable=protected-access
            ('ce_type', b'org.openedx.learning.auth.session.login.completed.v1'),
            ('ce_id', b'bad_id')
        ]
        with pytest.raises(UnusableMessageError) as excinfo:
            self.event_consumer.emit_signals_from_message(msg, self.signal)

        assert excinfo.value.args == (
            "Error determining metadata from message headers: badly formed hexadecimal UUID string",
        )

        assert not self.mock_receiver.called

    def test_no_deserializer_if_no_registry_client(self):
        with pytest.raises(Exception) as excinfo:
            get_deserializer(self.signal, None)
        assert excinfo.value.args == (
            "Cannot create Kafka deserializer -- missing library or settings",
        )


class TestCommand(TestCase):
    """
    Tests for the consume_events management command
    """

    @override_settings(EVENT_BUS_KAFKA_CONSUMERS_ENABLED=False)
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    def test_kafka_consumers_disabled(self, mock_create_consumer, mock_logger):
        call_command(Command(), topic='test', group_id='test')
        assert not mock_create_consumer.called
        mock_logger.error.assert_called_once_with("Kafka consumers not enabled, exiting.")

    @patch.dict('edx_event_bus_kafka.internal.consumer.__dict__', {'confluent_kafka': None})
    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    def test_kafka_consumers_no_lib(self, mock_create_consumer, mock_logger):
        call_command(Command(), topic='test', group_id='test')
        assert not mock_create_consumer.called
        mock_logger.error.assert_called_once_with(
            "Cannot consume events because confluent_kafka dependency (or one of its extras) was not installed"
        )

    @patch('edx_event_bus_kafka.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer.consume_indefinitely')
    def test_kafka_consumers_normal(self, mock_consume, mock_create_consumer, _gsbt):
        call_command(
            Command(),
            topic='test',
            group_id='test',
        )
        assert mock_create_consumer.called
        assert mock_consume.called

    @patch('edx_event_bus_kafka.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer.reset_offsets_and_sleep_indefinitely')
    def test_kafka_consumers_with_timestamp(self, mock_reset_offsets, mock_create_consumer, _gsbt):
        call_command(
            Command(),
            topic='test',
            group_id='test',
            offset_time=['2019-05-18T15:17:08.132263']
        )
        assert mock_create_consumer.called
        assert mock_reset_offsets.called

    @patch('edx_event_bus_kafka.internal.consumer.logger', autospec=True)
    @patch('edx_event_bus_kafka.internal.consumer.OpenEdxPublicSignal.get_signal_by_type')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer._create_consumer')
    @patch('edx_event_bus_kafka.internal.consumer.KafkaEventConsumer.consume_indefinitely')
    def test_kafka_consumers_with_bad_timestamp(self, _ci, _cc, _gsbt, mock_logger):
        call_command(Command(), topic='test', group_id='test', offset_time=['notatimestamp'])
        mock_logger.exception.assert_any_call("Could not parse the offset timestamp.")
        mock_logger.exception.assert_called_with("Error consuming Kafka events")
