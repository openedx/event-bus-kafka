from unittest import TestCase
from openedx_events.event_bus.avro.tests.test_utilities import create_simple_signal, SimpleAttrs
from openedx_events.event_bus.avro.serializer import AvroSignalSerializer

class TestEventProducer(TestCase):

    def test_magic(self):
        signal = create_simple_signal({
            "test": SimpleAttrs
        })
        serializer = AvroSignalSerializer(signal)
        key_serializer = magic(serializer.schema, "boolean_field")
        expected = {
            "name": "test",
            "type": "boolean",
            "doc": "key",
        }
        self.assertEqual()


