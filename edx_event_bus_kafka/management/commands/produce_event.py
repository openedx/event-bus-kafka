"""
Produce a single event. Intended for testing.

Implements required ``APP.management.commands.*.Command`` structure.
"""

import json
import logging

from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string

from edx_event_bus_kafka.internal.producer import create_producer

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Management command to produce a test event to the event bus.
    """
    help = """
    Produce a single test event with the given data to the specified Kafka topic.

    example:
        python3 manage.py cms produce_event --signal openedx_events.learning.signals.SESSION_LOGIN_COMPLETED \
          --topic user-login --key-field user.pii.username \
          --data '{"user": {
                    "id": 123,
                    "is_active": true,
                    "pii": {"username": "foobob", "email": "bob@foo.example", "name": "Bob Foo"}}}'
    """

    def add_arguments(self, parser):

        parser.add_argument(
            '--signal', nargs=1, required=True,
            help="Module:variable path to an OpenEdxPublicSignal instance",
        )
        parser.add_argument(
            '--topic', nargs=1, required=True,
            help="Topic to produce to (without environment prefix)",
        )
        parser.add_argument(
            '--key-field', nargs=1, required=True,
            help="Dotted string representing path to event key in event data dictionary",
        )
        parser.add_argument(
            '--data', nargs=1, required=True,
            help="JSON representation of kwargs dict appropriate for the signal",
        )

    def handle(self, *args, **options):
        try:
            producer = create_producer()
            producer.send(
                signal=import_string(options['signal'][0]),
                topic=options['topic'][0],
                event_key_field=options['key_field'][0],
                event_data=json.loads(options['data'][0]),
            )
            producer.prepare_for_shutdown()  # otherwise command may exit before delivery is complete
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error producing Kafka event")
