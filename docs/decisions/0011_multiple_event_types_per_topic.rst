10. Multiple event types per topic
##################################

Status
******

- **Accepted** DATE TO BE ADDED BEFORE MERGING

Context
*******

For the initial implementation of the event bus, we decided to limit each topic to use a single schema. This meant that every signal in openedx-events required a different topic. This worked for our initial use case, course catalog updates, because all changes were considered updates and could be emitted using the same signal.
However, other types of events are not so easily grouped into a single signal. For example, there are different signals for ``XBLOCK_PUBLISHED``, ``XBLOCK_UPDATED``, and ``XBLOCK_DUPLICATED``, and indeed different schemas across the three signals. Routing these signals through different topics could lead to events being processed in a nonsensical order, for example an xblock being duplicated before it's been published. See `Should You Put Several Event Types in the Same Kafka Topic?`_ and `Putting Several Event Types in the Same Topic – Revisited`_ for more information on why it can be useful to group different event types on the same topic.

.. _Should You Put Several Event Types in the Same Kafka Topic?: https://www.confluent.io/blog/put-several-event-types-kafka-topic/
.. _Putting Several Event Types in the Same Topic – Revisited: https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/

Decision
********
We will update the code for event serializers, which are responsible for registering and validating schemas against the schema registry, to use the TopicRecordName strategy.

TopicRecordName Strategy
========================
The Confluent Schema Registry stores schemas according to one of three strategies, referring to the identifiers under which the schemas are stored. When an event is serialized using an AvroSerializer, the serializer checks the schema against the registry, looking up the existing schema using the configured strategy. If the new schema is not allowable under the schema evolution rules, serialization will fail.

The default strategy the TopicName strategy. Under this strategy, the serializer will determine the existing schema by looking for a schema registered as ``<topic-name>-value`` (``<topic-name>-key`` when serializing the key), the topic name having been passed in the SerializationContext object. Therefore, if the serializer is given a new event type with the same topic, unless the new event type schema happens to be a valid evolution of the existing one, it will fail.

Using the TopicRecordName strategy, schemas are registered by topic and record name. On serializing an event, the serializer looks for a schema registered as ``<topic-name>-<record-name>``.
For example, given a schema like::

    {
        "doc": "Avro Event Format for CloudEvents created with openedx_events/schema",
        "fields": [{"name": "str_field", "type": "string"}]
        "name": "CloudEvent",
        "namespace": "my.event.type",
        "type": "record"
    }

and the topic ``my-topic``, the serializer will look for a schema registered as ``my-topic-my.event.type.CloudEvent`` and use that to determine if the schema being passed is valid. This means that as long as the namespace is different for the schemas for different event types, the serializer will not error when adding new event types. It will still maintain evolution rules within an event type, however, which is desirable behavior.

Consequences
************
* The record names for each event type will need to be distinct from each other. Originally, they were all just "CloudEvent." We needed to add namespaces to ensure that they were unique. Note that this will be necessary regardless of which solution we choose.
* Any event type will be able to be written to any topic
* Schema evolution rules will be enforced for event types but not for topics.
* The event-bus-kafka consumer will need to be updated to determine the signal from the message headers rather than taking a signal as a passed argument in the management command. This will also be necessary regardless of which solution we choose.


Rejected Alternatives
*********************

Setting the subject name strategy on the topic rather than the client
=====================================================================
This option is only available for locally-hosted or Dedicated Confluent Cloud clusters, and thus not a feasible solution for the whole community.

Using a setting toggle
======================
Usually we would put this sort of feature behind a SettingsToggle so deployers could enable it and disable it as they chose. However, because the subject name strategy is set at the client rather than topic or cluster level, all clients who produce to the same topic(s) must have the same subject name strategy set. While the consequences of having multiple clients with different subject name strategies writing to the same topic are not 100% clear, at the very least it would result in a proliferation of confusing schemas in the registry. 

Require related event types to have the same schema
===================================================
In theory we could avoid the issue of event types with different schemas on the same topic by simply not allowing them, requiring all event types that are intended to go on the same topic to have the same schema. This would require knowing in advance which event types will go to which topics and likely result in lots of extraneous fields which are only necessary for some of the event types and not others.

Avro Unions
===========
Avro unions are a datatype representing the possibility of multiple different schemas for a single field or record. They often contain references to other registered schemas. For example, setting a topic schema to [my.signal.CloudEvent, my.other.signal.CloudEvent] would allow events with either the my.signal.CloudEvent or my.other.signal.CloudEvent schema, but no others. This has the advantage of being configurable by topic and allowing greater control over which events are allowed on a topic.
Using Avro unions is currently not feasible because of `a bug`_ in the confluent-kafka-python library. It would also require schemas to be created, evolved, registered independently of event-producing code, requiring separate updates to configurations every time a new event type was added to a topic or we wanted to update an event schema.

.. _a bug: https://github.com/confluentinc/confluent-kafka-python/issues/1562







