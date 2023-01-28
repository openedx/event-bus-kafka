10. Audit logging
#################

Status
******

- **Accepted** 2023-02-07

Context
*******

We do not currently have a reliable source for information on the timestamps and processing time of Kafka messages. This information is necessary for a variety of purposes:

- Determining what timestamp to start replaying messages from, if a consumer or producer has misbehaved
- Investigating whether we have messages that are taking an excessively long time to process
- Checking the logs for what happened right *before* an error
- And just diagnosing bugs, in general

Our logs currently include some of that information, but only when an error occurs.

Decision
********

We will log at least minimal information about all Kafka messages that we produce and consume. These log messages will not necessarily have sufficient information to reconstruct a message, but will identify messages uniquely (topic, partition, and offset) and include timestamps that are necessary for reconstructing a timeline of activity. We also include message IDs, as they are necessary for uniquely identifying messages that are moved to other topics such as dead letter queues or retry queues. They may also be necessary if we move to using the Outbox Pattern, since messages will be created (receive an ID) separately from when they are produced (and receive an offset).

We will not include message values, as they are likely to contain PII or other sensitive information and would therefore affect how we handle these logs. We will also not include any other headers not necessary for identifying messages, as they are likely to increase log volume without much benefit. If full message details are needed, the audit log information should be sufficient for finding the messages in the cluster's control interface.

Timestamps will be recorded implicitly, via the logging framework's own log formatting.

Consequences
************

Log volume will be increased, likely not by too much.

Rejected Alternatives
*********************

The Kafka broker may have information about when the broker received the messages, but probably will not record when messages were consumed, or how long it took the consumer to process a message.

New Relic has some information, but due to sampling we don't have a complete picture. (Usually we record information for 99+% of events, but over an example 1 month window we see average capture drop as low as 70%. This may be due to heavier loss during replays.)
