8. Baseline error handling for the event bus
############################################

Status
******

Accepted

Context
*******

There are numerous strategies for error handling in Kafka, variously optimized for different goals (consistency, reliability, etc.). Some error-handling is built in to the confluent-kafka library, such as a built-in mechanism for automatically retrying messages a specified number of times when the producer is unable to reach the server. Other errors may occur outside the Kafka code, for example when trying to extract an event key from an event data dictionary.

Decisions
*********

- When dealing with errors in sending events to the event bus, we will prioritize reliability while ensuring that no events are irretrievably dropped.
- Failing to send an event to the event bus will not fail the request.
- We will catch exceptions and log the full event data in the error message in a consistent, parseable way.
- We will also send errors to the monitoring system, although the logs would be required to learn specifics about the message.
- We will defer any idea of using a retry queue.
- Note that this decision does not currently cover how we will handle errors on the consumer.

Consequences
************
- There may be inconsistencies in data between producing and consuming services when events are not sent.
- In rare cases such as server crashes some events could still conceivably be irretrievable.
- Resending failed events may require retrieving and parsing server logs. If other events have been successfully processed in the meantime, these resent events may be processed out of order.
- It will be up to the developer to determine how to resend errored events.
- Monitoring may still be used to alert when events are not being sent
