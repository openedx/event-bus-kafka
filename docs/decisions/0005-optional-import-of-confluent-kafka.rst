4. Optional import of confluent-kafka
#####################################

Status
******

Accepted

Context
*******

* `confluent-kafka`_ is a library written and maintained by Confluent, our managed instance provider (see :doc:`0004-kafka-managed-hosting`). The library abstracts out the work for sending and receiving events to and from the Kafka cluster and converting them into message objects.
* confluent-kafka in turn is a wrapper around a C library called `librdkafka`_ (distributed as `librdkafka_dev`)
* librdkafka-dev does not currently have a compiled binary available for Linux/aarch64

As a result of the points above, if a package includes a dependency on confluent-kafka, installation will fail in Linux/aarch64 environments. This is a particular problem for developers who are using Tutor on an M1 Mac. Tutor is the standard distribution for Open edX developers and maintainers, so this is a significant issue for a large part of the community.

.. _confluent-kafka: https://github.com/confluentinc/confluent-kafka-python
.. _librdkafka: https://github.com/edenhill/librdkafka

Decision
********

Instead of requiring confluent-kafka directly in base.in, we will wrap all imports of `confluent_kafka`in a `try...catch` block. If the import fails, the library will log an informative message and any calls will fail gracefully.

For example::

    try:
        import confluent_kafka
        from confluent_kafka import DeserializingConsumer
    except ImportError:
        confluent_kafka = None

Then, later on, before any usage of `DeserializingConsumer`::

    if not confluent_kafka:
        warn("Confluent_kafka not installed")
        return None
    ...do things with DeserializingConsumer

Consequences
************

This will make developers or other users of the edx-event-bus-kafka library responsible for installing confluent-kafka in their own environments.

For edx.org, we will install confluent-kafka as part of creating the docker containers that will run the services
that use this library.

Rejected Alternatives
*********************

* Make the entire `edx-event-bus-kafka` library an optional dependency in the services that use it (eg edx-platform, course-discovery)

This would require developers to install `edx-event-bus-kafka` separately when setting up their environment. This means it would not be able to be updated with `make upgrade` in the same way we manage versions of all of our other packages. Moreover, this would require separate commits to update the version of the package and update the code that uses it, meaning we would have to use an expand-contract release model for every breaking change. This goes against best practices, being highly error-prone.

We expect edx-event-bus-kafka to change more frequently than `confluent-kafka`, which is why we are more willing to adopt the optional dependency strategy for the latter.

* Keep both `confluent-kafka` and `edx-event-bus-kafka` as a required dependencies

While not necessarily causing problems for edx.org, this would break many community-hosted Open edX instances as well as many development environments.
