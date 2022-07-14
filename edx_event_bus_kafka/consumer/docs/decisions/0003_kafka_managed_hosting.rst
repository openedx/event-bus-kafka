Kafka Managed Hosting
=====================

Status
------

Provisional

Context
-------

* Setting up a `Kafka`_-based event bus can be complex since it comes in multiple parts that all need to be deployed
  and maintained together.

* The 2U/edX team responsible for this new infrastructure does not have the capacity to manage this in house.

.. _Kafka: https://kafka.apache.org/

Decision
--------

.. note::

    This decision is specific to edx.org and is not a requirement or recommendation for how other deployments in the Open edX community manage their brokers. However, the background may still be useful to other community members.

For edx.org, the initial deployment of `Kafka`_-based event bus will use the software as a servics (SaaS) provider `Confluent`_.

.. _Confluent: https://www.confluent.io/

Additional Background
---------------------

`Amazon MSK`_ is an AWS managed service that supplies the Apache Kafka core platform only.

The `Confluent Platform`_ adds additional capabilities, some of which are only commercially available.

  * `Schema Registry <https://www.confluent.io/product/confluent-platform/data-compatibility/>`__
  * Monitoring and alerting capabilities (Commercial)
  * Self-balancing clusters (Commercial)
  * Tiered storage (Commercial) (future feature of Apache Kafka)
  * Infinite retention (Commercial - Cloud only)

Also see a useful and biased `comparison of Apache Kafka vs Vendors`_ by Kai Waehner (of Confluent), comparing various providers and distributions of Kafka and related or competitive services. Or see `(Confluent biased) Amazon MSK vs Confluent Cloud <https://www.confluent.io/confluent-cloud-vs-amazon-msk>`__.

.. _Amazon MSK: https://aws.amazon.com/msk/
.. _Confluent Platform: https://www.confluent.io/product/confluent-platform
.. _comparison of Apache Kafka vs Vendors: https://www.kai-waehner.de/blog/2021/04/20/comparison-open-source-apache-kafka-vs-confluent-cloudera-red-hat-amazon-msk-cloud/
