9. Baseline error handling for the Kafka consumer
#################################################

Status
******

**Provisional**

Context
*******

There are a number of possible types of errors that the consumer may encounter in each iteration of its loop:

- Failure to get an event (network errors, broker or consumer configuration issues)
- Inability to parse a received event (can't talk to schema registry, schema mismatch, bug in serialization on producer)
- Event object that are unusable in some other way (bad or missing type headers, unexpected signal type)
- Errors returned by signal receivers

Some of these are temporary errors that will resolve on retry, such as network errors or signal receivers that encounter a network error making an outbound call. Some are permanent with respect to a given combination of event and consumer code/config, such as an incompatible serialization or schema. And some are permanent just with respect to the consumer itself, and would happen for all events, such as a bug in the consumer loop. For some errors we'll be able to tell which group they fall into (based on exception class or `Confluent Kafka error codes`_) but we cannot do this reliably for *all* errors. We also do not yet have much experience with Kafka and its failure modes on which to base decisions.

.. _Confluent Kafka error codes: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafkaerror

Additionally, the consumer has a limited number of options when it encounters an error. If it cannot retrieve an event, the best it can do is keep trying. If it cannot parse or identify a received event, it can either halt (wait for manual intervention) or start dropping events. And if a signal receiver is in error, the consumer can at best note this somehow.

Halting and waiting for intervention would be appropriate when the consumer is *completely broken*, but would be inappropriate when only a small subset of events cannot be processed. We may not be able to write code that can tell the difference between these situations. Depending on the deployment infrastructure, allowing an exception to propagate out of the consumer loop might result in the consumer being restarted (no overall effect), or it might shut down the consumer entirely until manually restarted (creating a halt).

Decision
********

We will start by catching *every* raised exception before it can escape the consumer loop, logging it with as much information as we think will be necessary for debugging or for restarting the consumer. There's currently no machinery for restarting a consumer group at a particular offset, but it would allow operators to recover from various types of failures (transient, data-specific, and buggy consumer) if needed.

Consumers of the Event Bus are generally expected to tolerate duplicate events (at-least-once delivery) and we know that some of the planned applications are predicated on low latency. Since topics can always be replayed to consumers, failing fast and moving on to the next event is preferable.

As we gain experience, we can add special handling for certain known cases that would be better served by a backoff, retry, or halt.

Receiver exceptions are not *raised*, per se, but are returned by ``send_robust``. We will log these but not treat these as consumer errors, as these exceptions are much more likely to be related to IDA business logic than to the Event Bus itself. There may also be multiple receivers, and failures in one do not indicate that other receivers should stop receiving the event stream.

All raised errors will also be sent to telemetry (New Relic) for monitoring.

Consequences
************

The consumer will err on the side of low latency between IDAs, creating higher (or at least sooner) inter-IDA consistency at the possible expense of inter-object consistency within a topic (if some events are dropped and others processed). We will have to ensure that we capture all failures sufficiently durably that we can replay them, and also that receivers are capable of "healing" their data when a topic is replayed.

Rejected Alternatives
*********************

Another common approach in eventing systems is the use of a Dead Letter Queue (DLQ). Events that cannot be processed are sent to a DLQ topic for later reprocessing. (Alternatively, they may be sent to a retry queue in the hopes that the error is transient, and only sent to the DLQ if the retry fails.) These approaches are still worth looking into, but have their own complexity (especially around message ordering) and the decision of whether to use these has been deferred. For now, the logged errors will serve as a crude DLQ.
