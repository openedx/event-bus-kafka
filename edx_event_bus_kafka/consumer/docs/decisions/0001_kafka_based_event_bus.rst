=====================
Kafka-Based Event Bus
=====================

Status
------

Provisional

Context
-------

The draft `OEP-52: Event Bus Architecture`_ explains how the Open edX platform would benefit from an event bus, as well as providing some additional decisions around the event bus. One decision is to enable the event bus technology to be pluggable through some abstraction layer.

This still requires selecting a specific technology for the first implementation.

.. _`OEP-52: Event Bus Architecture`: https://github.com/openedx/open-edx-proposals/pull/233

Decision
--------

An initial implementation of an event bus for the Open edX platform will be implemented using `Kafka`_. This implementation will be used by edx.org, and available to the Open edX community.

This decision does not preclude the introduction of alternative event bus implementations based on other technologies in the future.

.. _Kafka: https://kafka.apache.org/

Why Kafka?
~~~~~~~~~~

Kafka is a distributed streaming platform. Kafka's implementation maps nicely to the pub/sub pattern. However, some native features of a message broker are not built-in.

Kafka has been around for a long time. See `Thoughtworks's technology radar introduced Kafka`_ as "Assess" in 2015, and "Trial" in 2016. It never moved up to "Adopt", and also never moved down to "Hold". Read `Thoughtwork's Kafka decoder page`_ to learn more about its benefits and trade-offs, and how it is used.

More recently, the `Thoughtworks's technology radar introduced Apache Pulsar`_ as "assess" in 2020, and the `technology radar introduced Kafka API without Kafka`_ in 2021. This both demonstrates the de facto standard of the Kafka API, but also Thoughtwork's hope to find a less complex alternative.

We believe Apache Kafka is still the right option due to its maturity, documentation, support and community.

.. _Thoughtworks's technology radar introduced Kafka: https://www.thoughtworks.com/radar/tools/apache-kafka
.. _Thoughtwork's Kafka decoder page: https://www.thoughtworks.com/decoder/kafka

.. _Thoughtworks's technology radar introduced Apache Pulsar: https://www.thoughtworks.com/radar/platforms/apache-pulsar
.. _technology radar introduced Kafka API without Kafka: https://www.thoughtworks.com/radar/platforms/kafka-api-without-kafka

Kafka Highlights
~~~~~~~~~~~~~~~~

Pros
^^^^

* Battle-tested, widely adopted, big community, lots of documentation and answers.
* Enables event replay-ability.

Cons
^^^^

* Complex to manage, including likely manual scaling.
* Simple consumers require additional code for some messaging features.

Consequences
------------

* Operators will need to deploy and manage the selected infrastructure, which will likely be complex. If Apache Kafka is selected, there are likely to be a set of auxiliary parts to provide all required functionality for our message bus. However, third-party hosting is also available (see separate decision).
* Most of the consequences of an event bus should relate to `OEP-52: Event Bus Architecture`_ more generally, and hopefully will not be Kafka specific.

Rejected Alternatives
---------------------

Apache Pulsar
~~~~~~~~~~~~~

Although rejected for initial edx.org implementation, `Apache Pulsar`_ remains an option for those looking for an alternative to Kafka.

Pros
^^^^

* Ease of scalability (built-in, according to docs).
* Good data retention capabilities.
* Additional built-in pub/sub features (built-in, according to docs).

Cons
^^^^

* Requires 3rd party hosting or larger upfront investment if self-hosted (kubernetes).
* Less mature (but growing) community, little documentation, and few answers.
* Python built-in schema management is buggy and hard to work with for complex use cases.

Note: Read an interesting (Kafka/Confluent) biased article exploring `comparisons and myths of Kafka vs Pulsar`_.

.. _Apache Pulsar: https://pulsar.apache.org/
.. _comparisons and myths of Kafka vs Pulsar: https://dzone.com/articles/pulsar-vs-kafka-comparison-and-myths-explored

Redis
~~~~~

Pros
^^^^

* Already part of the Open edX platform.

Cons
^^^^

* Can lose acked data, even if RAM backed up with an append-only file (AOF).
* Requires homegrown schema management.

RabbitMQ
~~~~~~~~

Pros
^^^^

* Built-in message broker capabilities like routing, filtering, and fault handling.

Cons
^^^^

* Not built for message retention or message ordering.

AWS SNS/SQS
~~~~~~~~~~~

Pros
^^^^

* Simpler hosting for those self-hosting in AWS.

Cons
^^^^

* Cannot be shared as an open source solution.
* Events are not replayable.

Additional References
---------------------

* Technology comparisons performed by edX.org:

  * `Message Bus Rubric Definition <https://docs.google.com/document/d/1lKbOE8HkUk__Cyy5u_yFZ8ju0roPtlxcH1-9yf9hX8I/edit#>`__

  * `Message Bus Evaluation <https://docs.google.com/spreadsheets/d/1pA08DQ1h3bov5fL1KTrT0tk2RJseyxPsZCLJACtb3YY/edit#gid=0>`__

  * `Pulsar vs Kafka Hosting Comparison <https://openedx.atlassian.net/wiki/spaces/SRE/pages/3079733386>`__

* Third-party comparisons of Kafka vs Pulsar:

  * `(Kafka biased) Benchmarking comparison <https://www.confluent.io/blog/kafka-fastest-messaging-system/>`__
  * `(Pulsar biased) Performance, Architecture, and Features comparison - Part 1 <https://streamnative.io/en/blog/tech/2020-07-08-pulsar-vs-kafka-part-1/>`__
  * `(Pulsar biased) Performance, Architecture, and Features comparison - Part 2 <https://streamnative.io/en/blog/tech/2020-07-22-pulsar-vs-kafka-part-2/>`__
  * `(Kafka biased) Twitter's move from Pulsar-like to Kafka <https://blog.twitter.com/engineering/en_us/topics/insights/2018/twitters-kafka-adoption-story>`__

* Third-party comparisons of Kafka vs RabbitMQ:

  * `Blog article comparing Kafka and RabbitMQ <https://stiller.blog/2020/02/rabbitmq-vs-kafka-an-architects-dilemma-part-2/>`__
