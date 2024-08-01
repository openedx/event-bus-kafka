"""
Core consumer and event-loop code.
"""
import logging
import time
from datetime import datetime
from functools import lru_cache

from django.conf import settings
from django.db import connection
from django.dispatch import receiver
from django.test.signals import setting_changed
from edx_django_utils.cache import RequestCache
from edx_django_utils.monitoring import function_trace, record_exception, set_custom_attribute
from edx_toggles.toggles import SettingToggle
from openedx_events.event_bus import EventBusConsumer
from openedx_events.event_bus.avro.deserializer import AvroSignalDeserializer
from openedx_events.tooling import OpenEdxPublicSignal

from .config import get_full_topic, get_schema_registry_client, load_common_settings
from .utils import (
    AUDIT_LOGGING_ENABLED,
    HEADER_EVENT_TYPE,
    HEADER_ID,
    _get_metadata_from_headers,
    get_message_header_values,
    last_message_header_value,
)

logger = logging.getLogger(__name__)

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    import confluent_kafka
    from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer
    from confluent_kafka.error import KafkaError
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
except ImportError:  # pragma: no cover
    confluent_kafka = None

# .. toggle_name: EVENT_BUS_KAFKA_CONSUMERS_ENABLED
# .. toggle_implementation: SettingToggle
# .. toggle_default: True
# .. toggle_description: If set to False, consumer will exit immediately. This can be used as an emergency kill-switch
#   to disable a consumer—as long as the management command is killed and restarted when settings change.
# .. toggle_use_cases: opt_out
# .. toggle_creation_date: 2022-01-31
KAFKA_CONSUMERS_ENABLED = SettingToggle('EVENT_BUS_KAFKA_CONSUMERS_ENABLED', default=True)

# .. setting_name: EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT
# .. setting_default: 1.0
# .. setting_description: How long the consumer should wait, in seconds, for the Kafka broker
#   to respond to a poll() call.
CONSUMER_POLL_TIMEOUT = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT', 1.0)

# .. setting_name: EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP
# .. setting_default: 1.0
# .. setting_description: When the consumer fails to retrieve an event from the broker,
#   it will sleep for this many seconds before trying again. This is to prevent fast error-loops
#   if the broker is down or the consumer is misconfigured. It *may* also sleep for errors that
#   involve receiving an unreadable event, but this could change in the future to be more
#   specific to "no event received from broker".
POLL_FAILURE_SLEEP = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP', 1.0)


class UnusableMessageError(Exception):
    """
    Indicates that a message was successfully received but could not be processed.

    This could be invalid headers, an unknown signal, or other issue specific to
    the contents of the message.
    """


class ReceiverError(Exception):
    """
    Indicates that one or more receivers of a signal raised an exception when called.
    """

    def __init__(self, message: str, causes: list):
        """
        Create ReceiverError with a message and a list of exceptions returned by receivers.
        """
        super().__init__(message)
        self.causes = causes  # just used for testing


class EventConsumptionException(Exception):
    """
    Indicates that we had an issue in event production. Useful for filtering on later.
    """


def _reconnect_to_db_if_needed():
    """
    Reconnects the db connection if needed.

    This is important because Django only does connection validity/age checks as part of
    its request/response cycle, which isn't in effect for the consume-loop. If we don't
    force these checks, a broken connection will remain broken indefinitely. For most
    consumers, this will cause event processing to fail.
    """
    has_connection = bool(connection.connection)
    requires_reconnect = has_connection and not connection.is_usable()
    if requires_reconnect:
        connection.connect()


def _clear_request_cache():
    """
    Clear the RequestCache so that each event consumption starts fresh.

    Signal handlers may be written with the assumption that they are called in the context
    of a web request, so we clear the request cache just in case.
    """
    RequestCache.clear_all_namespaces()


def _prepare_for_new_work_cycle():
    """
    Ensure that the application state is appropriate for performing a new unit of work.

    This mimics some setup/teardown that is normally performed by Django in its
    request/response based architecture and that is needed for ensuring a clean and
    usable state in this worker-based application.
    """
    # Ensure that the database connection is active and usable.
    _reconnect_to_db_if_needed()

    # Clear the request cache, in case anything in the signal handlers rely on it.
    _clear_request_cache()


class KafkaEventConsumer(EventBusConsumer):
    """
    Construct consumer for the given topic and group. The consumer can then
    emit events from the event bus using the signal from the message headers.

    Note that the topic should be specified here *without* the optional environment prefix.

    Can also consume messages indefinitely off the queue.

    Attributes:
        topic: Topic to consume (without environment prefix).
        group_id: Consumer group id.
        consumer: Actual kafka consumer instance.
        offset_time: The timestamp (in ISO format) that we would like to reset the consumers to. If this is used, the
            consumers will only reset the offsets of the topic but will not actually consume and process any messages.
    """

    def __init__(self, topic, group_id, offset_time=None):
        if confluent_kafka is None:  # pragma: no cover
            raise Exception('Library confluent-kafka not available. Cannot create event consumer.')

        self.topic = topic
        self.group_id = group_id
        self.consumer = self._create_consumer()
        self.offset_time = None
        if offset_time:
            try:
                self.offset_time = datetime.fromisoformat(str(offset_time))
            except ValueError:
                logger.exception('Could not parse the offset timestamp.')
                raise
        self._shut_down_loop = False
        self.schema_registry_client = get_schema_registry_client()

    # return type Consumer removed from signature to avoid error on import
    def _create_consumer(self):
        """
        Create a Consumer in the correct consumer group

        Returns
            Consumer in the configured consumer group
        """

        consumer_config = load_common_settings()

        consumer_config.update({
            'group.id': self.group_id,
            # Turn off auto commit. Auto commit will commit offsets for the entire batch of messages received,
            # potentially resulting in data loss if some of those messages are not fully processed. See
            # https://newrelic.com/blog/best-practices/kafka-consumer-config-auto-commit-data-loss
            'enable.auto.commit': False,
        })

        return Consumer(consumer_config)

    def _shut_down(self):
        """
        Test utility for shutting down the consumer loop.
        """
        self._shut_down_loop = True

    def reset_offsets_and_sleep_indefinitely(self):
        """
        Reset any assigned partitions to the given offset, and sleep indefinitely.
        """

        def reset_offsets(consumer, partitions):
            # This is a callback method used on consumer assignment to handle offset reset logic.
            # We do not want to attempt to change offsets if the offset is None.
            if self.offset_time is None:
                return

            # Get the offset from the epoch. Kafka expects offsets in milliseconds for offsets_for_times. Although
            # this is undocumented in the libraries we're using (confluent-kafka and librdkafa), for reference
            # see the docs for kafka-python:
            # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.offsets_for_times
            offset_timestamp_ms = int(self.offset_time.timestamp()*1000)
            # We set the epoch timestamp in the offset position.
            for partition in partitions:
                partition.offset = offset_timestamp_ms

            partitions_with_offsets = consumer.offsets_for_times(partitions, timeout=1.0)

            # Partitions have an error field that may be set on return.
            errors = [p.error for p in partitions_with_offsets if p.error is not None]
            if len(errors) > 0:
                raise Exception("Error getting offsets for timestamps: {errors}")

            logger.info(f'Found offsets for timestamp {self.offset_time}: {partitions_with_offsets}')

            # We need to commit these offsets to Kafka in order to ensure these offsets are persisted.
            consumer.commit(offsets=partitions_with_offsets)

        full_topic = get_full_topic(self.topic)
        # Partition assignment will trigger the reset logic. This should happen on the first poll call,
        # but will also happen any time the broker needs to rebalance partitions among the consumer
        # group, which could happen repeatedly over the lifetime of this process.
        self.consumer.subscribe([full_topic], on_assign=reset_offsets)

        while True:
            # Allow unit tests to break out of loop
            if self._shut_down_loop:
                break

            # This log message may be noisy when we are replaying, but hopefully we only see it
            # once every 30 seconds.
            logger.info("Offsets are being reset. Sleeping instead of consuming events.")

            # We are calling poll here because we believe the offsets will not be set
            # correctly until poll is called, despite the offsets being reset in a different call.
            # This is because we don't believe that the partitions for the current consumer are assigned
            # until the first poll happens. Because we are not trying to consume any messages in this mode,
            # we are deliberately calling poll without processing the message it returns
            # or commiting the new offset.
            self.consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)

            time.sleep(30)
            continue

    def _consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop.
        """

        if not KAFKA_CONSUMERS_ENABLED.is_enabled():
            logger.error("Kafka consumers not enabled, exiting.")
            return

        # .. setting_name: EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT
        # .. setting_default: None
        # .. setting_description: If the consumer encounters this many consecutive errors, exit with an
        #   error. This is intended to be used in a context where a management system (such as Kubernetes)
        #   will relaunch the consumer automatically. The effect is that all runtime state is cleared,
        #   allowing consumers in arbitrary "stuck" states to resume their work automatically. (The impetus
        #   for this setting was a Django DB connection failure that was staying failed.) Process
        #   managers like Kubernetes will use delays and backoffs, so this may also help with transient
        #   issues such as networking problems or a burst of errors in a downstream service. Errors may
        #   include failure to poll, failure to decode events, or errors returned by signal handlers.
        #   This does not prevent committing of offsets back to the broker; any messages that caused an
        #   error will still be marked as consumed, and may need to be replayed.
        CONSECUTIVE_ERRORS_LIMIT = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT', None)

        try:
            full_topic = get_full_topic(self.topic)
            run_context = {
                'full_topic': full_topic,
                'consumer_group': self.group_id,
            }
            self.consumer.subscribe([full_topic])
            logger.info(f"Running consumer for {run_context!r}")

            # How many errors have we seen in a row? If this climbs too high, exit with error.
            # Any error counts, here — whether due to a polling failure or a message processing
            # failure. But only a successfully processed message clears the counter. Just
            # being able to talk to the broker and get a message (or a normal poll timeout) is
            # not sufficient to show that progress can be made.
            consecutive_errors = 0

            while True:
                # Allow unit tests to break out of loop
                if self._shut_down_loop:
                    break

                # Detect probably-broken consumer and exit with error.
                if CONSECUTIVE_ERRORS_LIMIT and consecutive_errors >= CONSECUTIVE_ERRORS_LIMIT:
                    raise Exception(f"Too many consecutive errors, exiting ({consecutive_errors} in a row)")

                with function_trace('consumer.consume'):
                    msg = None
                    try:
                        msg = self.consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)
                        if msg is not None:
                            # Before processing, try to make sure our application state is cleaned
                            # up as would happen at the start of a Django request/response cycle.
                            # See https://github.com/openedx/openedx-events/issues/236 for details.
                            _prepare_for_new_work_cycle()

                            signal = self.determine_signal(msg)
                            msg.set_value(self._deserialize_message_value(msg, signal))
                            self.emit_signals_from_message(msg, signal)
                            consecutive_errors = 0

                        self._add_message_monitoring(run_context=run_context, message=msg)
                    except Exception as e:
                        consecutive_errors += 1
                        self.record_event_consuming_error(run_context, e, msg)
                        # Kill the infinite loop if the error is fatal for the consumer
                        _, kafka_error = self._get_kafka_message_and_error(message=msg, error=e)
                        if kafka_error and kafka_error.fatal():
                            raise e
                        # Prevent fast error-looping when no event received from broker. Because
                        # DeserializingConsumer raises rather than returning a Message when it has an
                        # error() value, this may be triggered even when a Message *was* returned,
                        # slowing down the queue. This is probably close enough, though.
                        if msg is None:
                            time.sleep(POLL_FAILURE_SLEEP)
                    if msg:
                        # theoretically we could just call consumer.commit() without passing the specific message
                        # to commit all this consumer's current offset across all partitions since we only process one
                        # message at a time, but limit it to just the offset/partition of the specified message
                        # to be super safe
                        self.consumer.commit(message=msg)
        finally:
            self.consumer.close()

    def consume_indefinitely(self):
        """
        Consume events from a topic in an infinite loop if offset_time is not set else reset any assigned partitions to
        the given offset, and sleep indefinitely.
        """
        if self.offset_time is None:
            self._consume_indefinitely()
        else:
            self.reset_offsets_and_sleep_indefinitely()

    @function_trace('emit_signals_from_message')
    def emit_signals_from_message(self, msg, signal):
        """
        Send the event from the message via the given signal.

        Assumes the message has been deserialized and the signal matches the event_type of the message header.

        Arguments:
            msg (Message): Deserialized message.
            signal (OpenEdxPublicSignal): Signal - must match the event_type of the message header.
        """
        self._log_message_received(msg)

        if msg.error() is not None:
            raise UnusableMessageError(
                f"Polled message had error object: {msg.error()!r}"
            )

        # This should also never happen since the signal should be determined from the message
        # but it's here to prevent misuse of the method
        msg_event_type = self._get_event_type_from_message(msg)
        if signal.event_type != msg_event_type:
            raise Exception(f"Error emitting event from Kafka: (UNEXPECTED) message event type {msg_event_type}"
                            f" does not match signal {signal.event_type}")

        try:
            event_metadata = _get_metadata_from_headers(msg.headers())
        except Exception as e:
            raise UnusableMessageError(f"Error determining metadata from message headers: {e}") from e

        with function_trace('emit_signals_from_message_send_event_with_custom_metadata'):
            send_results = signal.send_event_with_custom_metadata(event_metadata, **msg.value())

        # Raise an exception if any receivers errored out. This allows logging of the receivers
        # along with partition, offset, etc. in record_event_consuming_error. Hopefully the
        # receiver code is idempotent and we can just replay any messages that were involved.
        self._check_receiver_results(send_results, signal)

        # At the very end, log that a message was processed successfully.
        # Since we're single-threaded, no other information is needed;
        # we just need the logger to spit this out with a timestamp.
        # See ADR: docs/decisions/0010_audit_logging.rst
        if AUDIT_LOGGING_ENABLED.is_enabled():
            logger.info('Message from Kafka processed successfully')

    def determine_signal(self, msg) -> OpenEdxPublicSignal:
        """
        Determine which OpenEdxPublicSignal should be used to emit the event data in a message

        Arguments:
            msg (Message): Consumed message

        Returns:
            The OpenEdxPublicSignal instance corresponding to the ce_type header on the message
        """
        event_type = self._get_event_type_from_message(msg)
        try:
            return OpenEdxPublicSignal.get_signal_by_type(event_type)
        except KeyError as ke:
            raise UnusableMessageError(
                f"Unrecognized type {event_type} found on message, cannot determine signal"
            ) from ke

    def _get_event_type_from_message(self, msg):
        """
        Return the event type from the ce_type header

        Arguments:
            msg (Message): the consumed message

        Returns
            The associated event type as a string
        """
        headers = msg.headers() or []  # treat None as []
        event_types = get_message_header_values(headers, HEADER_EVENT_TYPE)
        if len(event_types) == 0:
            raise UnusableMessageError(
                "Missing ce_type header on message, cannot determine signal"
            )
        if len(event_types) > 1:
            raise UnusableMessageError(
                "Multiple ce_type headers found on message, cannot determine signal"
            )
        return event_types[0]

    def _deserialize_message_value(self, msg, signal: OpenEdxPublicSignal):
        """
        Deserialize an Avro message value

        The signal is expected to match the ce_type header on the message

        Arguments:
            msg (Message): the raw message from the consumer
            signal (OpenEdxPublicSignal): The instance of OpenEdxPublicSignal corresponding to the ce_type header on msg

        Returns:
            The deserialized message value
        """
        msg_event_type = self._get_event_type_from_message(msg)
        if signal.event_type != msg_event_type:
            # This should never happen but it's here to prevent misuse of the method
            raise Exception(f"Error deserializing event from Kafka: (UNEXPECTED) message event type {msg_event_type}"
                            f" does not match signal {signal.event_type}")
        signal_deserializer = get_deserializer(signal, self.schema_registry_client)
        ctx = SerializationContext(msg.topic(), MessageField.VALUE, msg.headers())
        return signal_deserializer(msg.value(), ctx)

    def _check_receiver_results(self, send_results: list, signal: OpenEdxPublicSignal):
        """
        Raises exception if any of the receivers produced an exception.

        Arguments:
            send_results: Output of ``send_events``, a list of ``(receiver, response)`` tuples.
        """
        error_descriptions = []
        errors = []
        for signal_receiver, response in send_results:
            if not isinstance(response, BaseException):
                continue

            # Probably every receiver will be a regular function or even a lambda with
            # these attrs, so this check is just to be safe.
            try:
                receiver_name = f"{signal_receiver.__module__}.{signal_receiver.__qualname__}"
            except AttributeError:
                receiver_name = str(signal_receiver)

            # The stack traces are already logged by django.dispatcher, so just the error message is fine.
            error_descriptions.append(f"{receiver_name}={response!r}")
            errors.append(response)

        if len(error_descriptions) > 0:
            raise ReceiverError(
                f"{len(error_descriptions)} receiver(s) out of {len(send_results)} "
                "produced errors (stack trace elsewhere in logs) "
                f"when handling signal {signal}: {', '.join(error_descriptions)}",
                errors
            )

    def _log_message_received(self, msg):
        """
        Log that a message was received, for audit log purposes.

        See ADR: docs/decisions/0010_audit_logging.rst

        This will not be sufficient to reconstruct the message; it will just
        be enough to establish a timeline during debugging and to find a message
        by offset.
        """
        if not AUDIT_LOGGING_ENABLED.is_enabled():
            return

        try:
            message_id = last_message_header_value(msg.headers(), HEADER_ID)

            (ts_type, timestamp_ms) = msg.timestamp()
            if ts_type == TIMESTAMP_NOT_AVAILABLE:
                timestamp_info = "none"
            else:
                # Could be produce time or broker receive time; not going to bother to specify here.
                timestamp_info = str(timestamp_ms)

            # See ADR for details on why certain fields were included or omitted.
            logger.info(
                f'Message received from Kafka: topic={msg.topic()}, partition={msg.partition()}, '
                f'offset={msg.offset()}, message_id={message_id}, key={msg.key()}, '
                f'event_timestamp_ms={timestamp_info}'
            )
        except Exception as e:  # pragma: no cover
            # Use this to fix any bugs in what should be benign logging code
            set_custom_attribute('kafka_logging_error', repr(e))

    def record_event_consuming_error(self, run_context, error, maybe_message):
        """
        Record an error caught while consuming an event, both to the logs and to telemetry.

        Arguments:
            run_context: Dictionary of contextual information: full_topic, consumer_group,
              and expected_signal.
            error: An exception instance
            maybe_message: None if event could not be fetched or decoded, or a Kafka Message if
              one was successfully deserialized but could not be processed for some reason
        """
        context_msg = ", ".join(f"{k}={v!r}" for k, v in run_context.items())
        # Pulls the event message off the error for certain exceptions.
        maybe_kafka_message, _ = self._get_kafka_message_and_error(message=maybe_message, error=error)
        if maybe_kafka_message is None:
            event_msg = "no event available"
        else:
            event_details = {
                'partition': maybe_kafka_message.partition(),
                'offset': maybe_kafka_message.offset(),
                'headers': maybe_kafka_message.headers(),
                'key': maybe_kafka_message.key(),
                'value': maybe_kafka_message.value(),
            }
            event_msg = f"event details: {event_details!r}"

        try:
            # This is gross, but our record_exception wrapper doesn't take args at the moment,
            # and will only read the exception from stack context.
            raise EventConsumptionException(error)
        except EventConsumptionException:
            self._add_message_monitoring(run_context=run_context, message=maybe_kafka_message, error=error)
            record_exception()
            logger.exception(
                f"Error consuming event from Kafka: {error!r} in context {context_msg} -- {event_msg}"
            )

    def _add_message_monitoring(self, run_context, message, error=None):
        """
        Record additional details for monitoring.

        Arguments:
            run_context: Dictionary of contextual information: full_topic, consumer_group,
              and expected_signal.
            message: None if event could not be fetched or decoded, or a Message if one
              was successfully deserialized but could not be processed for some reason
            error: (Optional) An exception instance, or None if no error.
        """
        try:
            kafka_message, kafka_error = self._get_kafka_message_and_error(message=message, error=error)

            # .. custom_attribute_name: kafka_topic
            # .. custom_attribute_description: The full topic of the message or error.
            set_custom_attribute('kafka_topic', run_context['full_topic'])

            if kafka_message:
                # .. custom_attribute_name: kafka_partition
                # .. custom_attribute_description: The partition of the message.
                set_custom_attribute('kafka_partition', kafka_message.partition())
                # .. custom_attribute_name: kafka_offset
                # .. custom_attribute_description: The offset of the message.
                set_custom_attribute('kafka_offset', kafka_message.offset())
                headers = kafka_message.headers() or []  # treat None as []
                message_ids = get_message_header_values(headers, HEADER_ID)
                if len(message_ids) > 0:
                    # .. custom_attribute_name: kafka_message_id
                    # .. custom_attribute_description: The message id which can be matched to the logs. Note that the
                    #   header in the logs will use 'ce_id'.
                    set_custom_attribute('kafka_message_id', ",".join(message_ids))
                event_types = get_message_header_values(headers, HEADER_EVENT_TYPE)
                if len(event_types) > 0:
                    # .. custom_attribute_name: kafka_event_type
                    # .. custom_attribute_description: The event type of the message. Note that the header in the logs
                    #   will use 'ce_type'.
                    set_custom_attribute('kafka_event_type', ",".join(event_types))

            if kafka_error:
                # .. custom_attribute_name: kafka_error_fatal
                # .. custom_attribute_description: Boolean describing if the error is fatal.
                set_custom_attribute('kafka_error_fatal', kafka_error.fatal())
                # .. custom_attribute_name: kafka_error_retriable
                # .. custom_attribute_description: Boolean describing if the error is retriable.
                set_custom_attribute('kafka_error_retriable', kafka_error.retriable())

        except Exception as e:  # pragma: no cover
            # Use this to fix any bugs in what should be benign monitoring code
            set_custom_attribute('kafka_monitoring_error', repr(e))

    def _get_kafka_message_and_error(self, message, error):
        """
        Returns tuple of (kafka_message, kafka_error), if they can be found.

        Notes:
            * If the message was sent as a parameter, it will be returned.
            * If the message was not sent, and a KafkaException was sent, the
                message will be pulled from the exception if it exists.
            * A KafkaError will be returned if it is either passed directly,
                or if it was wrapped by a KafkaException.

        Arguments:
            message: None if event could not be fetched or decoded, or a Message if one
              was successfully deserialized but could not be processed for some reason
            error: An exception instance, or None if no error.
        """
        if not error or isinstance(error, KafkaError):
            return message, error

        kafka_error = getattr(error, 'kafka_error', None)
        # KafkaException uses args[0] to wrap the KafkaError
        if not kafka_error and len(error.args) > 0 and isinstance(error.args[0], KafkaError):
            kafka_error = error.args[0]

        kafka_message = getattr(error, 'kafka_message', None)
        if message and kafka_message and kafka_message != message:  # pragma: no cover
            # If this unexpected error ever occurs, we can invest in a better error message
            #   with a test, that includes event header details.
            logger.error("Error consuming event from Kafka: (UNEXPECTED) The event message did not match"
                         " the message packaged with the error."
                         f" -- event message={message!r}, error event message={kafka_message!r}.")
        # give priority to the passed message, although in theory, it should be the same message if not None
        kafka_message = message or kafka_message

        return kafka_message, kafka_error


# argument type SchemaRegistryClient for schema_registry_client removed from signature to avoid error on import
@lru_cache
def get_deserializer(signal: OpenEdxPublicSignal, schema_registry_client):
    """
    Get the value deserializer for a signal.

    This is cached in order to save work re-transforming classes into Avro schemas.
    We do not deserialize the key because we don't need it for anything yet.
    Also see https://github.com/openedx/openedx-events/issues/86 for some challenges on determining key schema.

    Arguments:
        signal: The OpenEdxPublicSignal to make a deserializer for.
        schema_registry_client: The SchemaRegistryClient instance for the consumer

    Returns:
        AvroSignalDeserializer for event value
    """
    if schema_registry_client is None:
        raise Exception('Cannot create Kafka deserializer -- missing library or settings')

    signal_deserializer = AvroSignalDeserializer(signal)

    def inner_from_dict(event_data_dict, ctx=None):  # pylint: disable=unused-argument
        return signal_deserializer.from_dict(event_data_dict)

    return AvroDeserializer(schema_str=signal_deserializer.schema_string(),
                            schema_registry_client=schema_registry_client,
                            from_dict=inner_from_dict)


@receiver(setting_changed)
def _reset_caches(sender, **kwargs):  # pylint: disable=unused-argument
    """Reset caches when settings change during unit tests."""
    get_deserializer.cache_clear()
