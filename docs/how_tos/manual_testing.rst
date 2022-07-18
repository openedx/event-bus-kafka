Manual testing
==============

The producer can be tested manually against a Kafka running in devstack.

#. Create a "unit test" in one of the test files that will actually call Kafka. For example, this could be added to the end of ``edx_event_bus_kafka/publishing/test_event_producer.py``::

    def test_actually_send_to_event_bus():
        import random
        signal = openedx_events.learning.signals.SESSION_LOGIN_COMPLETED
        # Make events distinguishable
        id = random.randrange(1000)
        event_data = {
            'user': UserData(
                id=id,
                is_active=True,
                pii=UserPersonalData(
                    username=f'foobob_{id:03}',
                    email='bob@foo.example',
                    name="Bob Foo",
                )
            )
        }

        print(f"Sending event with random user ID {id}.")
        with override_settings(
                SCHEMA_REGISTRY_URL='http://edx.devstack.schema-registry:8081',
                KAFKA_BOOTSTRAP_SERVERS='edx.devstack.kafka:29092',
        ):
            ep.send_to_event_bus(signal, 'user_stuff', 'user.id', event_data)

#. Make or refresh a copy of this repo where it can be seen from inside devstack: ``rsync -sax -delete ./ ../src/event-bus-kafka/``
#. In devstack, start Kafka and the control webapp: ``make dev.up.kafka-control-center`` and watch ``make dev.logs.kafka-control-center`` until server is up and happy (may take a few minutes; watch for ``INFO Kafka startTimeMs``)
#. Load the control center UI: http://localhost:9021/clusters and wait for the cluster to become healthy
#. In devstack, run ``lms-up-without-deps-shell`` to bring up an arbitrary shell inside Docker networking (LMS, in this case)
#. In the LMS shell, run ``pip install -e /edx/src/event-bus-kafka`` and then run whatever test you want, e.g. ``pytest /edx/src/event-bus-kafka/edx_event_bus_kafka/publishing/test_event_producer.py::test_actually_send_to_event_bus``
#. Go to the topic that was created and then into the Messages tab; select offset=0 to make sure you can see messages that were sent before you had the UI open.
#. Rerun ``rsync`` after any edits
