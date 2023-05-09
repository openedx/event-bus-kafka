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



Consequences
************


Rejected Alternatives
*********************


