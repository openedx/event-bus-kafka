Managing Kafka Consumers
========================

Status
------
Provisional

Context
-------
As outlined in the ADR on a Kafka-based Event Bus, edX.org has elected to go with Apache Kafka as our event bus implementation. Though the decision presented here is predicated on this particular edX.org decision, it is included to help other Open edX users evaluate Kafka for their own purposes. The standard pattern for consuming events with Kafka is to poll in a loop and process messages as they come in. According to the Confluent team it is a best practice to limit each consumer to a single topic (Confluent is a platform for industry-scale Kafka management)::

    consumer.subscribe(["topic"])
    while True:
        message = consumer.poll()
        ## process message

This ``while True`` loop means whatever is running this consumer will run infinitely and block whatever thread runs it from doing anything else. Thus, this code cannot be run as part of the regular Django web server. It also would not fit neatly onto a celery task, which would put it in direct competition for workers with all other celery tasks and be difficult to scale as the number of topics increases.

Decision
--------
edX.org will use Kubernetes to manage containers whose sole purpose is to run a management command, which in turn will run a polling loop against the specified topic. This will enable standard horizontal scaling of Kafka consumer groups.

The loop will listen for new events and then kick off the processing code for each event.

The new consumer containers will share access to the database and the same codebase as the backend service it supports. These would all be considered part of the same bounded context (from Domain-Driven Design).

Consequences
------------

* If the new consumer is supporting a backend service that is not yet deployed using Kubernetes, for example, as an AWS instance, care must be taken to ensure consumers are deployed with the rest of the app. Considerations need to be made if one part of this deployment breaks, but not another.

Rejected Alternatives
---------------------

#. Use a recurring/scheduled celery task to consume and process Kafka events

  * Celery has several disadvantages, including the difficulty of managing priorities for a queue, that it would be nice to avoid.
  * Horizontal scaling of consumer groups would not work correctly using our existing group of celery workers.
  * A fixed schedule for processing events would run into issues if the time to process events ever gets longer than the schedule.
  * Note: this may be used as a temporary solution for the purpose of iterating, but not as a long-term production solution.

#. Create a new ASG of EC2 instances dedicated to running a consumer management command, similar to how we create instances dedicated to running celery workers

  * edX and the industry in general we are moving away from the ASG pattern and on to Kubernetes. Both the ASG approach and the Kubernetes approach would require a substantial amount of work in order to make the number of instances scalable based on number of topics rather than built-in measurements like CPU load. Based on this, it makes more sense to put in the effort in Kubernetes rather than creating more outdated infrastructure.

#. Django-channels

  * Research turned up the possibility of using django-channels (websocket equivalent for Django) for use with Kafka, but the design and potential benefit was unclear so this was not pursued further
