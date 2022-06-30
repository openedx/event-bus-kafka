1. Purpose of this Repo
=======================

Status
------

Accepted

Context
-------

`OEP-52`_ (Event Bus Architecture) describes an event bus for the Open EdX platform that
allows for asynchronous messaging to improve both the efficiency and reliability
of the event messaging system.
This particular implementation of the event bus will use Kafka, although the event bus API should be
technology agnostic (API will live in a separate package). Implementation details on how the configuration
chooses which event bus to use is unspecified at this moment.

.. _OEP-52: https://open-edx-proposals.readthedocs.io/en/latest/architectural-decisions/oep-0052-arch-event-bus-architecture.html

Decision
--------

In this repository will reside the event bus kafka implementation as
described by OEP-52.

Since event bus kafka will be the first implementation, we will use this event bus
directly (API will be configured with kafka) and extract a more abstract API later.

Consequences
------------

All event bus related code will be moved to this repository at first.
