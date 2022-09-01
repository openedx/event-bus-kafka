6. Public API and App Organization
##################################


Status
******

Accepted

Context
*******

There has been no clear distinction between the public API in this package and the various internal, subject-to-change module attributes. This means we are more likely to end up with breaking changes, possibly without knowing it. Callers may also be unclear about which functions and classes they should be using.

Decision
********

Put most of the code into packages with ``internal`` in their name, and import the public bits into modules that are not marked that way.

- ``edx_event_bus_kafka/__init__.py`` imports various producer and consumer module attributes
- ``edx_event_bus_kafka/management/commands/*.py`` expose ``Command`` classes

These will also be documented in the main README.

The benefits of this setup include:

* A clear designation of what is part of the public API.
* The ability to refactor the implementation without changing the API.
* A clear reminder to developers adding new code that it needs to be exposed if it is public.
* A clear reminder to developers using the library not to use code from the internal implementation.

Consequences
************

Whenever a new class or function is added to the edx_django_utils public API, it should be implemented in the Django app's ``internal`` module and explicitly imported in its ``__init__.py`` module.

There will be some additional friction when a developer looks up the docstrings of a public API element, since they'll first have to open the public module and then find the internal module. Having the public docstrings in a module marked "internal" also creates some dissonance. Our prediction is that this dissonance will be outweighed by the benefits of API stability.

Rejected Alternatives
*********************

None.

References
**********

This draws heavily on an ADR in ``edx-django-utils``: `<https://github.com/openedx/edx-django-utils/blob/master/docs/decisions/0004-public-api-and-app-organization.rst>`_
