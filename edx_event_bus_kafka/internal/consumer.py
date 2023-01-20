"""
Core consumer and event-loop code.
"""

import logging
import time
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand
from edx_django_utils.monitoring import record_exception, set_custom_attribute
from edx_toggles.toggles import SettingToggle
from openedx_events.event_bus.avro.deserializer import AvroSignalDeserializer
from openedx_events.tooling import OpenEdxPublicSignal

from .config import get_full_topic, get_schema_registry_client, load_common_settings

logger = logging.getLogger(__name__)

# See https://github.com/openedx/event-bus-kafka/blob/main/docs/decisions/0005-optional-import-of-confluent-kafka.rst
try:
    import confluent_kafka
    from confluent_kafka import DeserializingConsumer
    from confluent_kafka.error import KafkaError
    from confluent_kafka.schema_registry.avro import AvroDeserializer
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

CONSUMER_POLL_TIMEOUT = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT', 1.0)

# .. setting_name: EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP
# .. setting_default: 1.0
# .. setting_description: When the consumer fails to retrieve an event from the broker,
#   it will sleep for this many seconds before trying again. This is to prevent fast error-loops
#   if the broker is down or the consumer is misconfigured. It *may* also sleep for errors that
#   involve receiving an unreadable event, but this could change in the future to be more
#   specific to "no event received from broker".
POLL_FAILURE_SLEEP = getattr(settings, 'EVENT_BUS_KAFKA_CONSUMER_POLL_FAILURE_SLEEP', 1.0)

# CloudEvent standard name for the event type header, see
# https://github.com/cloudevents/spec/blob/v1.0.1/kafka-protocol-binding.md#325-example
EVENT_ID_HEADER = "ce_id"
EVENT_TYPE_HEADER = "ce_type"


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


class KafkaEventConsumer:
    """
    Construct consumer for the given topic, group, and signal. The consumer can then
    emit events from the event bus using the configured signal.

    Can also consume messages indefinitely off the queue.
    """

    def __init__(self, topic, group_id, signal):
        if confluent_kafka is None:  # pragma: no cover
            raise Exception('Library confluent-kafka not available. Cannot create event consumer.')

        self.topic = topic
        self.group_id = group_id
        self.signal = signal
        self.consumer = self._create_consumer()
        self._shut_down_loop = False

    # return type (Optional[DeserializingConsumer]) removed from signature to avoid error on import
    def _create_consumer(self):
        """
        Create a DeserializingConsumer for events of the given signal instance.

        Returns
            None if confluent_kafka is not available.
            DeserializingConsumer if it is.
        """

        schema_registry_client = get_schema_registry_client()

        signal_deserializer = AvroSignalDeserializer(self.signal)

        def inner_from_dict(event_data_dict, ctx=None):  # pylint: disable=unused-argument
            return signal_deserializer.from_dict(event_data_dict)

        consumer_config = load_common_settings()

        # We do not deserialize the key because we don't need it for anything yet.
        # Also see https://github.com/openedx/openedx-events/issues/86 for some challenges on determining key schema.
        consumer_config.update({
            'group.id': self.group_id,
            'value.deserializer': AvroDeserializer(schema_str=signal_deserializer.schema_string(),
                                                   schema_registry_client=schema_registry_client,
                                                   from_dict=inner_from_dict),
            # Turn off auto commit. Auto commit will commit offsets for the entire batch of messages received,
            # potentially resulting in data loss if some of those messages are not fully processed. See
            # https://newrelic.com/blog/best-practices/kafka-consumer-config-auto-commit-data-loss
            'enable.auto.commit': False,
        })

        return DeserializingConsumer(consumer_config)

    def _shut_down(self):
        """
        Test utility for shutting down the consumer loop.
        """
        self._shut_down_loop = True

    def consume_indefinitely(self, offset_timestamp=None):
        """
        Consume events from a topic in an infinite loop.

        Arguments:
            offset_timestamp(datetime): reset the offsets of the consumer partitions to this timestamp before consuming.
        """

        def reset_offsets(consumer, partitions):
            # This is a callback method used on consumer assignment to handle offset reset logic.
            # We do not want to attempt to change offsets if the offset is None.
            if offset_timestamp is None:
                return

            # Get the offset from the epoch. Kafka expects offsets in milliseconds for offsets_for_times. Although
            # this is undocumented in the libraries we're using (confluent-kafka and librdkafa), for reference
            # see the docs for kafka-python:
            # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.offsets_for_times
            offset_timestamp_ms = int(offset_timestamp.timestamp()*1000)
            # We set the epoch timestamp in the offset position.
            for partition in partitions:
                partition.offset = offset_timestamp_ms

            partitions_with_offsets = consumer.offsets_for_times(partitions, timeout=1.0)

            # Partitions have an error field that may be set on return.
            errors = [p.error for p in partitions_with_offsets if p.error is not None]
            if len(errors) > 0:
                raise Exception("Error getting offsets for timestamps: {errors}")

            logger.info(f'Found offsets for timestamp {offset_timestamp}: {partitions_with_offsets}')

            consumer.assign(partitions_with_offsets)

        # This is already checked at the Command level, but it's possible this loop
        # could get called some other way, so check it here too.
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
                'expected_signal': self.signal,
            }
            self.consumer.subscribe([full_topic], on_assign=reset_offsets)
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

                # If offsets are set, do not consume events.
                if offset_timestamp is not None:
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

                # Detect probably-broken consumer and exit with error.
                if CONSECUTIVE_ERRORS_LIMIT and consecutive_errors >= CONSECUTIVE_ERRORS_LIMIT:
                    raise Exception(f"Too many consecutive errors, exiting ({consecutive_errors} in a row)")

                msg = None
                try:
                    msg = self.consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)
                    if msg is not None:
                        self.emit_signals_from_message(msg)
                        consecutive_errors = 0

                    self._add_message_monitoring(run_context=run_context, message=msg)
                except Exception as e:  # pylint: disable=broad-except
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

    def emit_signals_from_message(self, msg):
        """
        Determine the correct signal and send the event from the message.

        Arguments:
            msg (Message): Consumed message.
        """
        # DeserializingConsumer.poll() always returns either a valid message
        # or None, and raises an exception in all other cases. This means
        # we don't need to check msg.error() ourselves. But... check it here
        # anyway for robustness against code changes.
        if msg.error() is not None:
            raise UnusableMessageError(
                f"Polled message had error object (shouldn't happen): {msg.error()!r}"
            )

        headers = msg.headers() or []  # treat None as []

        event_types = [value for key, value in headers if key == EVENT_TYPE_HEADER]
        if len(event_types) == 0:
            raise UnusableMessageError(
                f"Missing {EVENT_TYPE_HEADER} header on message, cannot determine signal"
            )
        if len(event_types) > 1:
            raise UnusableMessageError(
                f"Multiple {EVENT_TYPE_HEADER} headers found on message, cannot determine signal"
            )

        event_type = event_types[0]

        # CloudEvents specifies using UTF-8 for header values, so let's be explicit.
        event_type_str = event_type.decode("utf-8")

        if event_type_str != self.signal.event_type:
            raise UnusableMessageError(
                f"Signal types do not match. Expected {self.signal.event_type}. "
                f"Received message of type {event_type_str}."
            )

        send_results = self.signal.send_event(**msg.value())
        # Raise an exception if any receivers errored out. This allows logging of the receivers
        # along with partition, offset, etc. in record_event_consuming_error. Hopefully the
        # receiver code is idempotent and we can just replay any messages that were involved.
        self._check_receiver_results(send_results)

    def _check_receiver_results(self, send_results: list):
        """
        Raises exception if any of the receivers produced an exception.

        Arguments:
            send_results: Output of ``send_events``, a list of ``(receiver, response)`` tuples.
        """
        error_descriptions = []
        errors = []
        for receiver, response in send_results:
            if not isinstance(response, BaseException):
                continue

            # Probably every receiver will be a regular function or even a lambda with
            # these attrs, so this check is just to be safe.
            try:
                receiver_name = f"{receiver.__module__}.{receiver.__qualname__}"
            except AttributeError:
                receiver_name = str(receiver)

            # The stack traces are already logged by django.dispatcher, so just the error message is fine.
            error_descriptions.append(f"{receiver_name}={response!r}")
            errors.append(response)

        if len(error_descriptions) > 0:
            raise ReceiverError(
                f"{len(error_descriptions)} receiver(s) out of {len(send_results)} "
                "produced errors (stack trace elsewhere in logs) "
                f"when handling signal {self.signal}: {', '.join(error_descriptions)}",
                errors
            )

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
            raise Exception(error)
        except BaseException:
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
                # header is list of tuples, so handle case with duplicate headers for same key
                message_ids = [value.decode("utf-8") for key, value in headers if key == EVENT_ID_HEADER]
                if len(message_ids) > 0:
                    # .. custom_attribute_name: kafka_message_id
                    # .. custom_attribute_description: The message id which can be matched to the logs. Note that the
                    #   header in the logs will use 'ce_id'.
                    set_custom_attribute('kafka_message_id', ",".join(message_ids))
                event_types = [value.decode("utf-8") for key, value in headers if key == EVENT_TYPE_HEADER]
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

        except Exception as e:  # pragma: no cover  pylint: disable=broad-except
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


class ConsumeEventsCommand(BaseCommand):
    """
    Management command for Kafka consumer workers in the event bus.
    """
    help = """
    Consume messages of specified signal type from a Kafka topic and send their data to that signal.

    Example::

        python3 manage.py cms consume_events -t user-login -g user-activity-service \
            -s org.openedx.learning.auth.session.login.completed.v1
    """

    def add_arguments(self, parser):

        parser.add_argument(
            '-t', '--topic',
            nargs=1,
            required=True,
            help='Topic to consume (without environment prefix)'
        )

        parser.add_argument(
            '-g', '--group_id',
            nargs=1,
            required=True,
            help='Consumer group id'
        )
        parser.add_argument(
            '-s', '--signal',
            nargs=1,
            required=True,
            help='Type of signal to emit from consumed messages.'
        )
        parser.add_argument(
            '-o', '--offset_time',
            nargs=1,
            required=False,
            default=None,
            help='The timestamp (in ISO format) that we would like to set the consumers to read from on startup. '
                  'Overrides existing offsets.'
        )

    def handle(self, *args, **options):
        if not KAFKA_CONSUMERS_ENABLED.is_enabled():
            logger.error("Kafka consumers not enabled, exiting.")
            return
        try:
            signal = OpenEdxPublicSignal.get_signal_by_type(options['signal'][0])
            if options['offset_time'] and options['offset_time'][0] is not None:
                try:
                    offset_timestamp = datetime.fromisoformat(options['offset_time'][0])
                except ValueError:
                    logger.exception('Could not parse the offset timestamp.')
                    raise
            else:
                offset_timestamp = None

            event_consumer = KafkaEventConsumer(
                topic=options['topic'][0],
                group_id=options['group_id'][0],
                signal=signal,
            )
            event_consumer.consume_indefinitely(offset_timestamp=offset_timestamp)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error consuming Kafka events")
