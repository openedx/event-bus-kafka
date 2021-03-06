"""
Produce a single event. Intended for testing.
"""

import json
import logging

from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string
from openedx_events.tooling import OpenEdxPublicSignal

from edx_event_bus_kafka.publishing.event_producer import send_to_event_bus

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Produce a single test event.
    """
    help = """
    This starts a Kafka event consumer that listens to the specified topic and logs all messages it receives. Topic
    is required.

    example:
        python3 manage.py cms produce_event --signal openedx_events.learning.signals.SESSION_LOGIN_COMPLETED \
          --topic user-event-debug --key-field user.pii.username \
          --data '{"user": {
                    "id": 123,
                    "is_active": true,
                    "pii": {"username": "foobob", "email": "bob@foo.example", "name": "Bob Foo"}}}'

    # TODO (EventBus): Potentially update example topic and group names to follow any future naming conventions.
    """

    def add_arguments(self, parser):

        parser.add_argument(
            '--signal', nargs=1, required=True,
            help="Module:variable path to an OpenEdxPublicSignal instance",
        )
        parser.add_argument(
            '--topic', nargs=1, required=True,
            help="Topic to consume",
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
            send_to_event_bus(
                signal=import_string(options['signal'][0]),
                topic=options['topic'][0],
                event_key_field=options['key_field'][0],
                event_data=json.loads(options['data'][0]),
                sync=True,  # otherwise command may exit before delivery is complete
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error producing Kafka event")
