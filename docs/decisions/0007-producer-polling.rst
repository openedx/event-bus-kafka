7. Polling the producer
#######################

Status
******

Accepted

Context
*******

Polling the producer is required in order to trigger delivery callbacks that are configured by the application. These callbacks are not triggered until the event is sent, ack'd, and finally polled. Even if callbacks are not configured, it may be necessary to call ``poll`` in order to burn down the delivery report queue.

The `recommendation`_ in the librdkafka docs is to call ``poll(0)`` before or after any event production, which is a parasitic approach ensuring that we're triggering callbacks at least as often as we're producing events. The events getting callbacks will _not_ be the ones just produced, since they won't have been acknowledged yet. However, if events are infrequent, it doesn't ensure that callbacks happen in a timely fashion, and the last event emitted before shutdown would never get a delivery callback.

The documentation offers a second strategy of just running a polling loop in a separate thread. This could be in addition to or instead of the parasitic polling. We don't have any important delivery callbacks configured at the moment, and it's not clear whether we ever will, or whether they'll be delay-sensitive—so it wasn't clear whether we should bother setting up a polling loop.

.. _recommendation: https://github.com/edenhill/librdkafka/wiki/FAQ#when-and-how-should-i-call-rd_kafka_poll

Decision
********

Even though it's not clear whether we'll need it, we'll run a separate thread with a polling loop, just since it's not that much additional code and we'll _probably_ want it.

We'll continue using the parasitic polling as well since it has the advantage of automatically adjusting the polling rate to the production rate without having to fine-tune the looping interval.

This is implemented in version 0.6.1.

Consequences
************

We have some code that we're not sure is needed, but at least shouldn't hurt.
