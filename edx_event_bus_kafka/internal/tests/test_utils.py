"""
Test header conversion utils
"""
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import uuid1

import attr
import ddt
import pytest
from django.test import TestCase, override_settings
from openedx_events.data import EventsMetadata

from edx_event_bus_kafka.internal.utils import (
    HEADER_EVENT_TYPE,
    HEADER_ID,
    HEADER_SOURCELIB,
    HEADER_TIME,
    _get_headers_from_metadata,
    _get_metadata_from_headers,
)

TEST_UUID = uuid1()


@ddt.ddt
class TestUtils(TestCase):
    """ Tests for header conversion utils """

    def test_headers_from_event_metadata(self):
        """
        Check we can generate message headers from an EventsMetadata object
        """
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
        """
        Check we can generate an EventsMetadata object from valid message headers
        """
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
            ('sourcelib', b'1.2.3'),
            ('minorversion', b'0')
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
        (TEST_UUID_BYTES, None, None, False),  # As long as we have a ce_id header, we can continue
        (b'bad', None, None, True),  # bad uuid
        (TEST_UUID_BYTES, b'bad', None, True),  # badly-formatted ce_time
        (TEST_UUID_BYTES, None, b'bad', True),  # badly-formatted sourcelib
        (None, None, None, True),
    )
    @ddt.unpack
    def test_generate_metadata_from_missing_or_bad_headers(self, msg_id, msg_time, source_lib, should_raise, mock_dt):
        """
        Check that we raise an exception iff there are missing required headers, or some of them are unparseable
        """
        now = datetime.now(timezone.utc)
        mock_dt.now = Mock(return_value=now)
        headers = filter(lambda x: x[1] is not None, [
            (HEADER_ID.message_header_key, msg_id),
            (HEADER_TIME.message_header_key, msg_time),
            (HEADER_SOURCELIB.message_header_key, source_lib),
            (HEADER_EVENT_TYPE.message_header_key, b'abc')
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

    def test_generate_metadata_fails_with_duplicate_headers(self):
        """
        Check that we raise if there are duplicate headers
        """
        headers = [
            (HEADER_ID.message_header_key, str(TEST_UUID).encode("utf-8")),
            (HEADER_ID.message_header_key, str(uuid1()).encode("utf-8")),
            (HEADER_EVENT_TYPE.message_header_key, b'abc')
        ]
        with pytest.raises(Exception) as exc_info:
            _get_metadata_from_headers(headers)

        assert exc_info.value.args == (
            "Multiple \"ce_id\" headers on message. Cannot determine correct metadata.",
        )
