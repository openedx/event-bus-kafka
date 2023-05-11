10. Multiple event types per topic
##################################

Status
******

**Accepted** DATE TO BE ADDED BEFORE MERGING

Context
*******

Following `this ADR`_ in openedx-events, all implementations of event bus must support multiple event types on the same topic. This ADR explains how we will implement this using Kafka.

Kafka offers the following possible solutions, all of which are explained in detail in the Decision and Rejected Alternatives sections:

#. Use the :ref:`TopicRecordName strategy<topic_record_name_strategy_client>` in the AvroSerializer
#. Set the TopicRecordName strategy :ref:`at the topic level<topic_record_name_strategy_topic>`
#. Use :ref:`Avro unions<avro_unions>`

.. _this ADR: https://openedx-events.readthedocs.io/en/latest/decisions/0010-multiple-event-types-per-topic.html

Decision
********
To comply with the event bus requirements, we will use the TopicRecordName strategy in the event serializers, which are responsible for registering and validating schemas against the schema registry.

.. _topic_record_name_strategy_client:

TopicRecordName strategy
========================
The Confluent Schema Registry stores schemas according to one of three strategies, referring to the identifiers under which the schemas are stored. When an event is serialized using an AvroSerializer, the serializer checks the schema against the registry, looking up the existing schema using the configured strategy. If the new schema is not allowable under the schema evolution rules, serialization will fail.

The default strategy is the TopicName strategy. Under this strategy, the serializer will determine the existing schema by looking for a schema registered as ``<topic-name>-value`` (``<topic-name>-key`` when serializing the key), the topic name having been passed in the SerializationContext object. Therefore, if the serializer is given a new event type with the same topic, unless the new event type schema happens to be a valid evolution of the existing one, it will fail.

Using the TopicRecordName strategy, schemas are registered by topic and record name. The record name is set from the schema as ``<namespace>.<name>`` (or just ``<name>`` if no namespace is provided). On serializing an event, the serializer looks for a schema registered as ``<topic-name>-<record-name>``.
For example, given a schema like::

    {
        "doc": "Avro Event Format for CloudEvents created with openedx_events/schema",
        "fields": [{"name": "str_field", "type": "string"}]
        "name": "CloudEvent",
        "namespace": "my.event.type",
        "type": "record"
    }

and the topic ``my-topic``, the serializer will look for a schema registered as ``my-topic-my.event.type.CloudEvent`` and use that to determine if the schema being passed is valid. This means that as long as the namespace is different for the schemas for different event types, the serializer will not error when adding new event types. It will still maintain evolution rules within an event type, however, which is desirable behavior.

Rationale
=========
Setting the TopicRecordName strategy on the event serializers is a very simple code change that requires no fundamental changes in how we produce events. It still allows us to take advantage of the automatic schema registration and evolution features of the AvroSerializer. Moreover, it does not depend on how the Kafka cluster is hosted and is thus appropriate for the entire community.

Consequences
************
* The record names for each event type will need to be distinct from each other. Originally, they were all just "CloudEvent." We needed to add namespaces to ensure that they were unique. Note that this will be necessary regardless of which solution we choose.
* All topics will accept any event type. This will be true even for topics where we only want a single event type.
* Schema evolution rules will be enforced separately for each event type on a topic.
* The event-bus-kafka consumer will need to be updated to determine the signal from the message headers rather than taking a signal as a passed argument in the management command. This will also be necessary regardless of which solution we choose.


Rejected Alternatives
*********************

.. _topic_record_name_strategy_topic:

Setting the subject name strategy on the topic rather than the client
=====================================================================
Some Kafka clusters allow operators to set the subject name strategy on a per-topic rather than per-client basis. This would have the advantage of allowing us to continue to restrict certain topics to only one event type.

Reason for rejection
--------------------
This option is only available for locally-hosted or Dedicated Confluent Cloud clusters, and thus not a feasible solution for the whole community.

Using different serializers based on the topic
==============================================
We could refactor the code to configure the AvroSerializer differently based on the topic, only changing the subject name strategy when the topic should allow multiple event types.

Reason for rejection
--------------------
 This would be a significant amount of work and would require some list of which topics accept multiple event types, which would have to be maintained and would reduce the flexibility of the event bus.

.. _avro_unions:

Avro Unions
===========
Avro unions are a datatype representing the possibility of multiple different schemas for a single field or record. They often contain references to other registered schemas. For example, setting a topic schema to [my.signal.CloudEvent, my.other.signal.CloudEvent] would allow events with either the my.signal.CloudEvent or my.other.signal.CloudEvent schema, but no others. This has the advantage of being configurable by topic and allowing greater control over which events are allowed on a topic.

Reason for rejection
--------------------
Using Avro unions is currently not feasible because of `a bug`_ in the confluent-kafka-python library. It would also require all schemas, not just the union ones, to be created, evolved, registered independently of event-producing code, requiring separate updates to configurations every time a new event type was added to a topic or we wanted to update an event schema. This is because, in order to use unions, auto-registration of schemas must be disabled, which is done on a per-serializer basis. As mentioned previously, all serializers have the same configurations. Changing this would be a significant lift.

.. _a bug: https://github.com/confluentinc/confluent-kafka-python/issues/1562
