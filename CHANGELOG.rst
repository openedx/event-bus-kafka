Change Log
##########

..
   All enhancements and patches to edx_event_bus_kafka will be documented
   in this file.  It adheres to the structure of https://keepachangelog.com/ ,
   but in reStructuredText instead of Markdown (for ease of incorporation into
   Sphinx documentation and the PyPI description).
   
   This project adheres to Semantic Versioning (https://semver.org/).

.. There should always be an "Unreleased" section for changes pending release.

Unreleased
**********

*

[0.4.0] - 2022-08-15
********************

Changed
=======

* Rename settings to have consistent prefix.

  * ``KAFKA_CONSUMERS_ENABLED`` becomes ``EVENT_BUS_KAFKA_CONSUMERS_ENABLED``
  * ``CONSUMER_POLL_TIMEOUT`` becomes ``EVENT_BUS_KAFKA_CONSUMER_POLL_TIMEOUT``
  * Updates to documentation and tests for various settings previously renamed

[0.3.1] - 2022-08-11
********************

Changed
=======

* Refactored consumer to use common configuration.

[0.3.0] - 2022-08-10
********************

Changed
=======

* Moved configuration onto separate file.
* Updated configuration settings to have EVENT_BUS_KAFKA prefix.

[0.2.0] - 2022-08-09
********************

Fixed
=====

* Cache producers so that they don't lose data.

[0.1.0] - 2022-06-16
********************

Added
=====

* First release on PyPI.
