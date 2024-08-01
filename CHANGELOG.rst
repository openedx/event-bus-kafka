Change Log
##########

..
   All enhancements and patches to edx_event_bus_kafka will be documented
   in this file.  It adheres to the structure of https://keepachangelog.com/ ,
   but in reStructuredText instead of Markdown (for ease of incorporation into
   Sphinx documentation and the PyPI description).

   This project adheres to Semantic Versioning (https://semver.org/).

.. There should always be an "Unreleased" section for changes pending release.

Unreleased
**********

[5.8.0] - 2024-08-01
********************
Changed
=======
* Monitoring: Ensure that we have a root span for each iteration of the consume loop; renamed the trace name to be ``consumer.consume``.

[5.7.0] - 2024-03-22
********************
Changed
=======
* Dropped Support for Django 3.2, Django 4.0 and Django 4.1
* Added Support for Python 3.12

[5.6.0] - 2024-01-25
********************
Changed
=======
* Added client.id to base configuration.

[5.5.0] - 2023-09-21
********************
Changed
=======
* Reset edx-django-utils RequestCache before handling each event

[5.4.0] - 2023-08-28
********************
Changed
=======
* Changed ordering of certain context assignments in producer code.
* Adds custom exceptions for producing and consuming errors.

[5.3.1] - 2023-08-10
********************
Fixed
=====
* Constrain openedx-events version to at least 8.5.0 (for EventsMetadata serialization support)

[5.3.0] - 2023-08-08
********************
Changed
=======
* Added event_metadata_as_dict to ProducingContext for easier replay from logs

[5.2.0] - 2023-08-03
********************
Changed
=======
* Added event_data_as_dict to ProducingContext for easier replay from logs

[5.1.0] - 2023-05-17
********************
Changed
=======
* Reconfigured serializers to use topic_record_name_strategy, allowing multiple event types per topic

[5.0.0] - 2023-05-17
********************
Changed
=======
* **BREAKING CHANGE**: Removed deprecated ``signal`` argument from consumer initialization

[4.0.1] - 2023-05-10
********************
Fixed
=====
* Added ``signal`` back as an argument when creating a consumer for compatibility with the openedx-events API

[4.0.0] - 2023-05-10
********************
Changed
=======
* Implement openedx-events Event Bus Consumer API.
* **BREAKING CHANGE**: Remove ``consume_events`` management command as this command will be provided by openedx_events. To replay events using the
  openedx-events version of the management command, pass ``--extra '{"offset_time": "2023-01-08T06:46:22"}'`` instead of ``-o 2023-01-08T06:46:22``.
* **BREAKING Change**: ``offset_timestamp`` argument has been removed from ``consume_indefinitely`` and ``reset_offsets_and_sleep_indefinitely`` methods.
  It is now added as an optional argument named ``offset_time`` to ``KafkaEventConsumer`` constructor.

[3.10.0] - 2023-05-05
*********************
Changed
=======
* Switch from ``edx-sphinx-theme`` to ``sphinx-book-theme`` since the former is
  deprecated
* Refactored consumer to manually deserialize messages instead of using DeserializingConsumer
* Make signal argument optional in consumer command (take signal from message headers)

[3.9.6] - 2023-02-24
********************
Added
=====
* Add function tracing to the event consumption workflow for better monitoring.

[3.9.5] - 2023-02-24
********************

Fixed
=====
* ``consume_events`` command now loads all public signals so that the consumer can load signals by ``event_type`` even if their modules were not already imported

[3.9.4] - 2023-02-16
********************

Fixed
=====
* produce_event management command fixed to pass required events_metadata parameter

[3.9.3] - 2023-02-10
********************
Fixed
=====
* Include ``message_id`` in audit log when message is produced (was ``None``)

[3.9.2] - 2023-02-08
********************
Fixed
=====
* Added documentation to all Django settings used in consumer and producer

[3.9.1] - 2023-02-07
********************
There was no version 3.9.0, due to a release issue. (Ignore any ``v3.9.0`` tag.)

Added
=====
* Added logging of successful message production, consumption, and processing (not just errors)

[3.8.1] - 2023-02-03
********************
Changed
=======
* Require and use openedx-events 5.0.0, which has a breaking API change that affects event bus consumers.

[3.8.0] - 2023-01-31
********************
Added
=====
* Producer now passes all metadata fields as headers
* Consumer emits events with the original metadata information (from the producer)

[3.7.1] - 2023-01-31
********************
Fixed
=====
* Consumer management command exits with useful error message if confluent-kafka library not available.

[3.7.0] - 2023-01-30
********************
Changed
=======
* Added ``reset_offsets_and_sleep_indefinitely`` method to consumer; relying code should switch to calling this when an offset timestamp is set.
* Deprecated the ``offset_timestamp`` parameter on the consumer's ``consume_indefinitely`` method, since ``reset_offsets_and_sleep_indefinitely`` should be used instead.

[3.6.3] - 2023-01-27
********************
Fixed
=====
* In the consumer loop, open new database connection if the old one has closed before processing messages

[3.6.2] - 2023-01-26
********************
Fixed
=====
* Reset mode now commits the correct offsets to Kafka.

[3.6.1] - 2023-01-20
********************
Fixed
=======
* Added a poll call to force resets to be processed during replay/offset-reset mode.

[3.6.0] - 2023-01-06
********************
Changed
=======
* Consumers do not consume events after resetting offsets.

[3.5.1] - 2023-01-06
********************
Fixed
=====
* Convert timestamps to millisecond offsets as expected by the Kafka API when replaying by timestamp in the consumer

[3.5.0] - 2023-01-05
********************
Added
=====
* New setting ``EVENT_BUS_KAFKA_CONSUMER_CONSECUTIVE_ERRORS_LIMIT`` will terminate the consumer if too many errors occur in a row, supporting an automated version of "have you tried turning it off and on again" (as long as consumer will automatically be restarted e.g. by Kubernetes).

[3.4.1] - 2022-12-20
********************
Fixed
=====
* Fixed bugs in the event replay/offset handling code for consumers.

[3.4.0] - 2022-12-16
********************
Changed
=======
* Kill infinite consumer loop when we see a fatal KafkaError, as recommended in the documentation. See https://github.com/confluentinc/librdkafka/blob/e0b9e92a0b492b5b1a6f1bcf08744928d45bf396/INTRODUCTION.md#fatal-consumer-errors.

[3.3.0] - 2022-12-15
********************
Changed
=======
* Added/removed some custom attributes used for monitoring. Search for custom_attribute_name annotations for details.

[3.2.0] - 2022-12-14
********************
Changed
=======
* Add timestamp parameter for consumer, allowing the starting offset for consuming to be overridden from the default.

[3.1.0] - 2022-12-07
********************

Added
=====
* A variety of custom attributes are now set for monitoring purposes. Search for custom_attribute_name annotations for details.

Changed
=======
* Error recording to the log will now include message details when the message is available on the error.

[3.0.0] - 2022-12-02
********************
Changed
=======
* **BREAKING CHANGE**: Make event_metadata parameter required

[2.1.0] - 2022-12-01
********************
Changed
=======
* Set CloudEvent headers on events using an optional event_metadata parameter

[2.0.0] - 2022-11-28
********************
Changed
=======
* Implement openedx-events Event Bus Producer API
* **BREAKING CHANGE**: Remove caching from ``get_producer`` and rename to ``create_producer``, as we now rely on the wrapper in openedx-events to cache that call

Upgrading library from 1.x:

- Replace calls to ``edx_event_bus_kafka.get_producer`` with ``openedx_events.event_bus.get_producer``
- Add Django setting ``EVENT_BUS_PRODUCER = "edx_event_bus_kafka.create_producer"``

These breaking changes are only relevant for the producing side. (This should only include the CMS at the moment.)

[1.10.0] - 2022-11-21
*********************
Changed
=======
* Improve receiver error log message -- mention that stack traces are elsewhere in log

[1.9.0] - 2022-11-15
********************
Changed
=======
* Log and record receiver errors the same way as other errors (with offset, partition, etc.)

[1.8.1] - 2022-11-10
********************
Changed
=======
* Commit consumer offset asynchronously

[1.8.0] - 2022-11-09
********************
Added
=====
* Consumer logs a warning for receivers that fail with an exception

[1.7.0] - 2022-11-04
********************

Changed
=======
* Manually manage commits instead of using auto-commit on the consumer
* Catch Exception instead of BaseException on both producer and consumer

[1.6.0] - 2022-11-04
********************

Changed
=======
* Enhanced error logging in consumer, including telemetry for exceptions
* Consumer loop will no longer exit when an error is encountered

[1.5.0] - 2022-11-01
********************

Changed
=======
* Log full event data on all producer errors

[1.4.3] - 2022-10-31
********************

Fixed
=====
* Upgrade openedx-events and fastavro to bring in a fix for schema creation

[1.4.2] - 2022-10-31
********************

Fixed
=====
* Removed proof-of-concept code that logged user-login events

[1.4.1] - 2022-10-28
********************

Fixed
=====
* Correct and clarify management command help strings (some copy-paste errors)
* Update TODO comments

[1.4.0] - 2022-10-21
********************

Changed
=======
* Remove override of auto.offset.reset on consumer (which will default to "latest"). New consumer groups will consume only messages that are sent after the group was initialized.
* Remove redundant lookup of signal in consumer loop (should not have any effect)
* Explicitly encode message header values as UTF-8 (no change in behavior)

[1.3.0] - 2022-10-20
********************

Changed
=======

* Upgrade openedx-events. When AvroSignalSerializer gets event schemas, it will get whatever is currently defined in openedx-events, so this will update the COURSE_CATALOG_EVENT_CHANGED schema (dropping `effort` field)

[1.2.0] - 2022-10-13
********************

Changed
=======

* ``EVENT_BUS_KAFKA_CONSUMERS_ENABLED`` now defaults to True instead of False
* Removed manual monitoring since New Relic tracks these now.

[1.1.0] - 2022-10-06
********************

Changed
=======

* Added monitoring for consumption tasks.

[1.0.0] - 2022-10-03
********************

Changed
=======

* Fixed bug in schema registry that was sending schemas to the wrong topic
* Bump version to 1.x to acknowledge that this is in use in production

[0.7.0] - 2022-09-08
********************

Changed
=======

* **Breaking changes** ``EventProducerKafka`` is now ``KafkaEventProducer``
* KafkaEventConsumer is now part of the public API

[0.6.2] - 2022-09-08
********************

Added
=====

* Topic names can be autoprefixed by setting ``EVENT_BUS_TOPIC_PREFIX``

[0.6.1] - 2022-09-06
********************

Added
=====

* Producer now polls on an interval, improving callback reliability. Configurable with ``EVENT_BUS_KAFKA_POLL_INTERVAL_SEC``.

[0.6.0] - 2022-09-01
********************

Changed
=======

* **Breaking change**: Public API is now defined in ``edx_event_bus_kafka`` package and ``edx_event_bus_kafka.management.commands`` package; all other modules should be considered unstable and not for external use.

[0.5.1] - 2022-08-31
********************

Fixed
=====

* Various lint issues (and missing ``__init__.py`` files.)

[0.5.0] - 2022-08-31
********************

Changed
=======

* **Breaking changes** in the producer module, refactored to expose a better API:

  * Rather than ``send_to_event_bus(...)``, relying code should now call ``get_producer().send(...)``.
  * The ``sync`` kwarg is gone; to flush and sync messages before shutdown, call ``get_producer().prepare_for_shutdown()`` instead.

* Clarify that config module is for internal use only.
* Implementation changes: Only a single Producer is created, and is used for all signals.

[0.4.4] - 2022-08-26
********************

Fixed
=====

* Fixed bug in test module for when confluent-kafka isn't present

[0.4.3] - 2022-08-24
********************

Fixed
=====

* Never evict producers from cache. There wasn't a real risk of this, but now we can rely on them being long-lived. Addresses remainder of `<https://github.com/openedx/event-bus-kafka/issues/16>`__.

[0.4.2] - 2022-08-24
********************

Fixed
=====

* Properly load auth settings for producer/consumer. (Auth settings were ignored since 0.3.1.)

[0.4.1] - 2022-08-18
********************

Changed
=======

* Remove confluent-kafka as a formal dependency of the repository.

  * Note: This library will not work without confluent-kafka.

* Add an ADR to explain why this work was done.

[0.4.0] - 2022-08-15
********************

Changed
=======

* Rename settings to have consistent prefix.

  * ``KAFKA_CONSUMERS_ENABLED`` becomes ``EVENT_BUS_KAFKA_CONSUMERS_ENABLED``
  * ``CONSUMER_POLL_TIMEOUT`` becomes ``EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT``
  * Updates to documentation and tests for various settings previously renamed

[0.3.1] - 2022-08-11
********************

Changed
=======

* Refactored consumer to use common configuration.

[0.3.0] - 2022-08-10
********************

Changed
=======

* Moved configuration onto separate file.
* Updated configuration settings to have EVENT_BUS_KAFKA prefix.

[0.2.0] - 2022-08-09
********************

Fixed
=====

* Cache producers so that they don't lose data.

[0.1.0] - 2022-06-16
********************

Added
=====

* First release on PyPI.
