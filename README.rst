edX Event Bus - Kafka
#####################

Kafka implementation for Open edX event bus.

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge|

Overview
********
This package implements an event bus for Open EdX using Kafka.

The event bus acts as a broker between services publishing events and other services that consume these events.
Implementing the event bus will allow for asynchronous messaging across services which greatly improves efficiency as we don't have to wait for scheduled batch synchronizations.
Additionally, since the services all speak to the event bus, they are independent of one another and can still function if one service crashes.

This package contains both the publishing code, which processes events into messages to send to the broker, and the consumer code,
which polls the broker using a `while True` loop in order to turn messages back into event data to be emitted.
The actual Kafka host will be configurable.

The goal for event-bus-kafka is to eventually have a flexible event bus that can be easily brought into other apps and repositories to produce and consume arbitrary topics.
Ideally, the event bus itself will also be an abstraction behind which platform maintainers can use non-Kafka implementations (Redis, Pulsar, etc.).
The documentation/ADRs may also be moved to more appropriate places as the process matures.

There are a hefty number of "# TODO (EventBus)" annotations left in to help guide further development.
While still under development, this app will be subject to frequent and rapid changes.
Outside of testing this app, it is best to leave the ``EVENT_BUS_KAFKA_CONSUMERS_ENABLED`` setting off.

The repository works together with the openedx/openedx-events repository to make the fully functional event bus.

For manual testing, see `<docs/how_tos/manual_testing.rst>`__.

Documentation
*************

- Main API: ``edx_event_bus_kafka`` exposes ``get_producer`` and a Producer API class. See `<https://github.com/openedx/openedx-events/issues/87>`_ for how these will be documented and used in the future.
- Django management commands: ``edx_event_bus_kafka.management.commands.*`` expose ``Command`` classes

OEP-52 documentation: https://open-edx-proposals.readthedocs.io/en/latest/architectural-decisions/oep-0052-arch-event-bus-architecture.html
(TODO: `Set up documentation <https://openedx.atlassian.net/wiki/spaces/DOC/pages/21627535/Publish+Documentation+on+Read+the+Docs>`_)

Development Workflow
********************

One Time Setup
==============
.. code-block::

  # Clone the repository
  git clone git@github.com:openedx/event-bus-kafka.git
  cd event-bus-kafka

  # Set up a virtualenv using virtualenvwrapper with the same name as the repo and activate it
  mkvirtualenv -p python3.8 event-bus-kafka


Every time you develop something in this repo
=============================================
.. code-block::

  # Activate the virtualenv
  workon event-bus-kafka

  # Grab the latest code
  git checkout main
  git pull

  # Install/update the dev requirements
  make requirements

  # Run the tests and quality checks (to verify the status before you make any changes)
  make validate

  # Make a new branch for your changes
  git checkout -b <your_github_username>/<short_description>

  # Using your favorite editor, edit the code to make your change.
  vim …

  # Run your new tests
  pytest ./path/to/new/tests

  # Run all the tests and quality checks
  make validate

  # Commit all your changes
  git commit …
  git push

  # Open a PR and ask for review.

Installation
************

This library is not intended to be a direct dependency of any service. Instead, it will likely be installed by some private mechanism. Unless the platform gains new processes for installing private dependencies, upgrades will need to be manually managed via a pin.

License
*******

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

How To Contribute
*****************

Contributions are very welcome.
Please read `How To Contribute <https://github.com/openedx/edx-platform/blob/master/CONTRIBUTING.rst>`_ for details.
Even though they were written with ``edx-platform`` in mind, the guidelines
should be followed for all Open edX projects.

The pull request description template should be automatically applied if you are creating a pull request from GitHub. Otherwise you
can find it at `PULL_REQUEST_TEMPLATE.md <.github/PULL_REQUEST_TEMPLATE.md>`_.

The issue report template should be automatically applied if you are creating an issue on GitHub as well. Otherwise you
can find it at `ISSUE_TEMPLATE.md <.github/ISSUE_TEMPLATE.md>`_.

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@edx.org.

Getting Help
************

If you're having trouble, we have discussion forums at https://discuss.openedx.org where you can connect with others in the community.

Our real-time conversations are on Slack. You can request a `Slack invitation`_, then join our `community Slack workspace`_.

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx-slack-invite.herokuapp.com/
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

.. |pypi-badge| image:: https://img.shields.io/pypi/v/edx-event-bus-kafka.svg
    :target: https://pypi.python.org/pypi/edx-event-bus-kafka/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/openedx/event-bus-kafka/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/openedx/event-bus-kafka/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/openedx/event-bus-kafka/coverage.svg?branch=main
    :target: https://codecov.io/github/openedx/event-bus-kafka?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/edx-event-bus-kafka/badge/?version=latest
    :target: https://edx-event-bus-kafka.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/edx-event-bus-kafka.svg
    :target: https://pypi.python.org/pypi/edx-event-bus-kafka/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/openedx/event-bus-kafka.svg
    :target: https://github.com/openedx/event-bus-kafka/blob/main/LICENSE.txt
    :alt: License
