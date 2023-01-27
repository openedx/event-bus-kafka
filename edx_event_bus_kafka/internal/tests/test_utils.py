"""
Test header conversion utils
"""
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import uuid1

import attr
import ddt
import pytest
from attrs import asdict, fields, filters
from django.test import TestCase, override_settings
from openedx_events.data import EventsMetadata

from edx_event_bus_kafka.internal.utils import (
    EVENT_TYPE_HEADER_KEY,
    ID_HEADER_KEY,
    MINORVERSION_HEADER_KEY,
    SOURCE_HEADER_KEY,
    SOURCEHOST_HEADER_KEY,
    SOURCELIB_HEADER_KEY,
    TIME_HEADER_KEY,
    _get_headers_from_metadata,
    _get_metadata_from_headers,
)

TEST_UUID = uuid1()


@ddt.ddt
class TestUtils(TestCase):
    """ Tests for header conversion utils """

    def test_headers_from_event_metadata(self):
        with override_settings(SERVICE_VARIANT='test'):
            metadata = EventsMetadata(event_type="org.openedx.learning.auth.session.login.completed.v1",
                                      id=TEST_UUID,
                                      sourcelib=(1, 2, 3),
                                      sourcehost="host",
                                      minorversion=0,
                                      time=datetime.fromisoformat("2023-01-01T14:00:00+00:00"))
            headers = _get_headers_from_metadata(event_metadata=metadata)
            self.assertDictEqual(headers, {
                'ce_type': b'org.openedx.learning.auth.session.login.completed.v1',
                'ce_id': str(TEST_UUID).encode("utf8"),
                'ce_source': b'openedx/test/web',
                'ce_specversion': b'1.0',
                'sourcehost': b'host',
                'content-type': b'application/avro',
                'ce_datacontenttype': b'application/avro',
                'ce_time': b'2023-01-01T14:00:00+00:00',
                'sourcelib': b'1.2.3',
                'ce_minorversion': b'0',
            })

    def test_metadata_from_headers(self):
        uuid = uuid1()
        headers = [
            ('ce_type', b'org.openedx.learning.auth.session.login.completed.v1'),
            ('ce_id', str(uuid).encode("utf8")),
            ('ce_source', b'openedx/test/web'),
            ('ce_specversion', b'1.0'),
            ('sourcehost', b'testsource'),
            ('content-type', b'application/avro'),
            ('ce_datacontenttype', b'application/avro'),
            ('ce_time', b'2023-01-01T14:00:00+00:00'),
            ('sourcelib', b'1.2.3')
        ]
        generated_metadata = _get_metadata_from_headers(headers)
        expected_metadata = EventsMetadata(
            event_type="org.openedx.learning.auth.session.login.completed.v1",
            id=uuid,
            minorversion=0,
            source='openedx/test/web',
            sourcehost='testsource',
            time=datetime.fromisoformat("2023-01-01T14:00:00+00:00"),
            sourcelib=(1, 2, 3),
        )
        self.assertDictEqual(attr.asdict(generated_metadata), attr.asdict(expected_metadata))

    TEST_UUID_BYTES = str(TEST_UUID).encode("utf8")

    @patch('edx_event_bus_kafka.internal.utils.oed.datetime')
    @ddt.data(
        (TEST_UUID_BYTES, None, None, False),
        (b'bad-id', None, None, True),
        (TEST_UUID_BYTES, b'0000', None, True),
        (TEST_UUID_BYTES, None, b'bananas', True),
        (None, None, None, True),
    )
    @ddt.unpack
    def test_metadata_from_missing_or_bad_headers(self, msg_id, msg_time, source_lib, should_raise, mock_dt):
        now = datetime.now(timezone.utc)
        mock_dt.now = Mock(return_value=now)
        headers = filter(lambda x: x[1] is not None, [
            (ID_HEADER_KEY, msg_id),
            (TIME_HEADER_KEY, msg_time),
            (SOURCELIB_HEADER_KEY, source_lib),
            (EVENT_TYPE_HEADER_KEY, b'abc')
        ])
        if should_raise:
            with pytest.raises(Exception):
                _get_metadata_from_headers(headers)
        else:
            # check that we use all the regular EventsMetadata defaults for missing fields by constructing one
            # and comparing it to the one generated from _get_metadata_from_headers
            expected_metadata = EventsMetadata(event_type="abc", id=TEST_UUID)
            generated_metadata = _get_metadata_from_headers(headers)
            self.assertDictEqual(attr.asdict(generated_metadata), attr.asdict(expected_metadata))
