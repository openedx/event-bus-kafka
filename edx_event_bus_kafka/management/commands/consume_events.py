"""
Makes ``consume_events`` management command available.

Implements required ``APP.management.commands.*.Command`` structure.
"""

from edx_event_bus_kafka.consumer.event_consumer import ConsumeEventsCommand as Command  # pylint: disable=unused-import
