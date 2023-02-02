"""
Utilities for converting between message headers and EventsMetadata
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple
from uuid import UUID

import openedx_events.data as oed

logger = logging.getLogger(__name__)


def _sourcelib_tuple_to_str(sourcelib: Tuple):
    return ".".join(map(str, sourcelib))


def _sourcelib_str_to_tuple(sourcelib_as_str: str):
    return tuple(map(int, sourcelib_as_str.split(".")))


class MessageHeader:
    """
    Utility class for converting between message headers and EventsMetadata objects
    """
    _mapping = {}
    instances = []

    def __init__(self, message_header_key, event_metadata_field=None, to_metadata=None, from_metadata=None):
        self.message_header_key = message_header_key
        self.event_metadata_field = event_metadata_field
        self.to_metadata = to_metadata or (lambda x: x)
        self.from_metadata = from_metadata or (lambda x: x)
        self.__class__.instances.append(self)
        self.__class__._mapping[self.message_header_key] = self


HEADER_EVENT_TYPE = MessageHeader("ce_type", event_metadata_field="event_type")
HEADER_ID = MessageHeader("ce_id", event_metadata_field="id", from_metadata=str, to_metadata=UUID)
HEADER_SOURCE = MessageHeader("ce_source", event_metadata_field="source")
HEADER_SPEC_VERSION = MessageHeader("ce_specversion")
HEADER_TIME = MessageHeader("ce_time", event_metadata_field="time",
                            to_metadata=lambda x: datetime.fromisoformat(x),  # pylint: disable=unnecessary-lambda
                            from_metadata=lambda x: x.isoformat())
HEADER_MINORVERSION = MessageHeader("ce_minorversion", event_metadata_field="minorversion", to_metadata=int,
                                    from_metadata=str)

# not CloudEvent headers, so no "ce" prefix
HEADER_SOURCEHOST = MessageHeader("sourcehost", event_metadata_field="sourcehost")
HEADER_SOURCELIB = MessageHeader("sourcelib", event_metadata_field="sourcelib",
                                 to_metadata=_sourcelib_str_to_tuple, from_metadata=_sourcelib_tuple_to_str)

# The documentation is unclear as to which of the following two headers to use for content type, so for now
# use both
HEADER_CONTENT_TYPE = MessageHeader("content-type")
HEADER_DATA_CONTENT_TYPE = MessageHeader("ce_datacontenttype")


def _get_metadata_from_headers(headers: List[Tuple]):
    """
    Create an EventsMetadata object from the headers of a Kafka message

    Arguments
        headers: The list of headers returned from calling message.headers() on a consumed message

    Returns
        An instance of EventsMetadata with the parameters from the headers. Any fields missing from the headers
         are set to the defaults of the EventsMetadata class
    """
    # Transform list of (header, value) tuples to a {header: [list of values]} dict. Necessary as an intermediate
    # step because there is no guarantee of unique headers in the list of tuples
    headers_as_dict = defaultdict(list)
    metadata_kwargs = {}
    for key, value in headers:
        headers_as_dict[key].append(value)

    # go through all the headers we care about and set the appropriate field
    for header in MessageHeader.instances:
        metadata_field = header.event_metadata_field
        if not metadata_field:
            continue
        header_key = header.message_header_key
        header_values = headers_as_dict[header_key]
        if len(header_values) == 0:
            # the id is required, everything else we make optional for now
            if header_key == HEADER_ID.message_header_key:
                raise Exception(f"Missing \"{header_key}\" header on message, cannot continue")
            logger.warning(f"Missing \"{header_key}\" header on message, will use EventsMetadata default")
            continue
        if len(header_values) > 1:
            raise Exception(
                f"Multiple \"{header_key}\" headers on message. Cannot determine correct metadata."
            )
        header_value = header_values[0].decode("utf-8")
        metadata_kwargs[header.event_metadata_field] = header.to_metadata(header_value)
    return oed.EventsMetadata(**metadata_kwargs)


def _get_headers_from_metadata(event_metadata: oed.EventsMetadata):
    """
    Create a dictionary of CloudEvent-compliant Kafka headers from an EventsMetadata object.

    This method assumes the EventsMetadata object was the one sent with the event data to the original signal handler.

    Arguments:
        event_metadata: An EventsMetadata object sent by an OpenEdxPublicSignal

    Returns:
        A dictionary of headers where the keys are strings and values are binary
    """
    values = {
        # Always 1.0. See "Fields" in OEP-41
        HEADER_SPEC_VERSION.message_header_key: b'1.0',
        HEADER_CONTENT_TYPE.message_header_key: b'application/avro',
        HEADER_DATA_CONTENT_TYPE.message_header_key: b'application/avro',
    }
    for header in MessageHeader.instances:
        if not header.event_metadata_field:
            continue
        event_metadata_value = getattr(event_metadata, header.event_metadata_field)
        # CloudEvents specifies using UTF-8; that should be the default, but let's make it explicit.
        values[header.message_header_key] = header.from_metadata(event_metadata_value).encode("utf8")

    return values
