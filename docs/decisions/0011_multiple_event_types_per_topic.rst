10. Multiple event types per topic
##################################

Status
******

- **Accepted** 2023-05-08

Context
*******

For the initial implementation of the event bus, we decided to limit each topic to use a single schema. This meant that every signal in openedx-events required a different topic. This worked for our initial use case, course catalog updates, because all changes were considered updates and could be emitted using the same signal.
However, other types of events are not so easily grouped into a single signal. For example, there are different signals for XBLOCK_PUBLISHED, XBLOCK_UPDATED, and XBLOCK_DUPLICATED, and indeed different schemas across the three signals. However, routing these signals through different topics could lead to events being processed in a nonsensical order, for example an xblock being duplicated before it's been published.


Decision
********
We will provide a configuration option EVENT_BUS_KAFKA_MULTIPLE_EVENT_TYPES_PER_TOPIC that will allow event producers to send events of different types to the same topic. When the setting is enabled, event serializers, which are responsible for **communicating** with the schema registry, will use the TopicRecordName strategy when registering and verifying schemas.

TopicRecordName Strategy
========================
The Confluent Schema Registry stores schemas according to one of three strategies, referring to the identifiers under which the schemas are stored. When an event is serialized using an AvroSerializer, the serializer checks the schema against the registry, looking up the existing schema using the configured strategy. If the new schema is not allowable under the schema evolution rules, serialization will fail.

The default strategy the TopicName strategy. Under this strategy, the serializer will determine the existing schema by looking for a schema registered as <topic-name>-value (<topic-name>-key when serializing the key), the topic name having been passed in the SerializationContext. Therefore, if the serializer is given a new event type with the same topic, unless the new event type schema happens to be a valid evolution of the existing one, it will fail.

Using the TopicRecordName strategy, schemas are registered by topic and record name. On serializing an event, the serializer looks for a schema registered as <topic-name>-<record-name>.
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
* The record names for each event type will need to be distinct from each other. Originally, they were all just "CloudEvent." We needed to add namespaces to ensure that they were unique.
* For any given service, either all topics it writes to will allow multiple event types or none will. It's possible this may be adjusted with cluster settings.
* Any event type will be able to be written to any topic
* Schema evolution rules will be enforced for event types but not for topics


Rejected Alternatives
*********************
Require related event types to have the same schema
===================================================
In theory we could avoid the issue of event types with different schemas on the same topic by simply not allowing them, requiring all event types that are intended to go on the same topic to have the same schema. This would require knowing in advance which event types will go to which topics and likely result in lots of extraneous fields which are only necessary for some of the event types and not others.

Avro Unions
===========
Avro unions are a datatype representing the possibility of multiple different schemas for a single field or record. They often contain references to other registered schemas. For example, setting a topic schema to [my.signal.CloudEvent, my.other.signal.CloudEvent] would allow events with either the my.signal.CloudEvent or my.other.signal.CloudEvent schema, but no others. This has the advantage of being configurable by topic and allowing greater control over which events are allowed on a topic.
Using Avro unions is currently not feasible because of a bug in the confluent-kafka-python library. It would also require schemas to be created and registered independently of event-producing code, requiring separate updates to configurations every time a new event type was added to a topic.









