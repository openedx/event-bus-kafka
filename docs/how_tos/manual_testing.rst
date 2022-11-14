Manual testing
##############

The producer can be tested manually against a Kafka running in devstack.

Setting up for testing
======================

#. Make or refresh a copy of this repo where it can be seen from inside devstack: ``rsync -sax -delete ./ ../src/event-bus-kafka/`` (and rerun rsync after any edits as needed)
#. In devstack, start Kafka and the control webapp: ``make dev.up.kafka-control-center`` and watch ``make dev.logs.kafka-control-center`` until server is up and happy (may take a few minutes; watch for ``INFO Kafka startTimeMs``)
#. Load the control center UI: http://localhost:9021/clusters and wait for the cluster to become healthy
#. Create the topic you want to use for testing. For the examples used in the management commands, you'll want to use ``dev-user-login`` with default settings.
#. In edx-platform's ``cms/envs/common.py``, add ``'edx_event_bus_kafka'`` to the ``INSTALLED_APPS`` list
#. In devstack, run ``make devpi-up studio-up-without-deps-shell`` to bring up Studio with a shell.
#. In the Studio shell, run ``pip install -e /edx/src/event-bus-kafka``
#. In the Studio shell, run ``pip install 'confluent_kafka[avro,schema-registry]'`` (necessary external dependency)

(Any IDA should work for testing, but all interactions have to happen inside devstack's networking layer, otherwise Kafka can't talk to itself.)

Testing the consumer
====================

The consumer may not read older events from the topic—and it never will for the first run of a topic/group pair—so you may need to started it before the producer, or re-run the producer after the consumer is started.

- Run the example command listed in the ``edx_event_bus_kafka.consumer.event_consumer.ConsumeEventsCommand`` docstring
- Once an event comes in, expect to see output that ends with a line containing "Received SESSION_LOGIN_COMPLETED signal with user_data"

Testing the producer
====================

Note: If you're also running the consumer, you'll need to do this in a separate studio shell.

- Run the example command listed in the ``edx_event_bus_kafka.management.commands.produce_event.Command`` docstring
- Expect to see output that ends with a line containing "Event delivered to topic"
- Go to the topic that was created and then into the Messages tab; select offset=0 to make sure you can see messages that were sent before you had the UI open.
