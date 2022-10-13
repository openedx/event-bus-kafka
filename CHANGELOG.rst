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

*

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
