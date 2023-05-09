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


Consequences
************
* The record names for each event type will need to be distinct from each other. Originally, they were all just "CloudEvent." We needed to add namespaces to ensure that they were unique.
* For any given service, either all topics it writes to will allow multiple event types or none will. It's possible this may be adjusted with cluster settings.
* Any event type will be able to be written to any topic


Rejected Alternatives
*********************
Avro Unions
===========
Avro unions are a datatype representing the possibility of multiple different schemas for a single field or record. They often contain references to other registered schemas. For example, setting a topic schema to [my.signal.CloudEvent, my.other.signal.CloudEvent] would allow events with either the my.signal.CloudEvent or my.other.signal.CloudEvent schema, but no others. This has the advantage of being configurable by topic and allowing greater control over which events are allowed on a topic.
Using Avro unions is currently not feasible because of a bug in the confluent-kafka-python library. It would also require schemas to be created and registered independently of event-producing code, requiring separate updates to configurations every time a new event type was added to a topic.

Require related event types to have the same schema
===================================================





