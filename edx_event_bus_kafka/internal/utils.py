"""
Utilities for converting between message headers and EventsMetadata
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple
from uuid import UUID

import openedx_events.data as oed

EVENT_TYPE_HEADER_KEY = "ce_type"
ID_HEADER_KEY = "ce_id"
SOURCE_HEADER_KEY = "ce_source"
SPEC_VERSION_HEADER_KEY = "ce_specversion"
TIME_HEADER_KEY = "ce_time"
MINORVERSION_HEADER_KEY = "ce_minorversion"

# not CloudEvent headers, so no "ce" prefix
SOURCEHOST_HEADER_KEY = "sourcehost"
SOURCELIB_HEADER_KEY = "sourcelib"

# The documentation is unclear as to which of the following two headers to use for content type, so for now
# use both
CONTENT_TYPE_HEADER_KEY = "content-type"
DATA_CONTENT_TYPE_HEADER_KEY = "ce_datacontenttype"

logger = logging.getLogger(__name__)

# We don't use the ce_type header when creating the EventsMetadata object because that is dealt with separately in the
# consumer loop
HEADER_KEY_TO_EVENTSMETADATA_FIELD = {
    EVENT_TYPE_HEADER_KEY: 'event_type',
    ID_HEADER_KEY: 'id',
    MINORVERSION_HEADER_KEY: 'minorversion',
    SOURCE_HEADER_KEY: 'source',
    SOURCEHOST_HEADER_KEY: 'sourcehost',
    TIME_HEADER_KEY: 'time',
    SOURCELIB_HEADER_KEY: 'sourcelib'
}


def _sourcelib_tuple_to_str(sourcelib: Tuple):
    return ".".join(map(str, sourcelib))


def _sourcelib_str_to_tuple(sourcelib_as_str: str):
    return tuple(map(int, sourcelib_as_str.split(".")))


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
    for header_key, metadata_field in HEADER_KEY_TO_EVENTSMETADATA_FIELD.items():
        header_values = headers_as_dict[header_key]
        if len(header_values) == 0:
            # the id is required, everything else we make optional for now
            if header_key == ID_HEADER_KEY:
                raise Exception(f"Missing \"{header_key}\" header on message, cannot continue")
            logger.warning(f"Missing \"{header_key}\" header on message, will use EventMetadata default")
            continue
        if len(header_values) > 1:
            raise Exception(
                f"Multiple \"{header_key}\" headers on message. Cannot determine correct metadata."
            )
        header_value = header_values[0].decode("utf-8")
        # some headers require conversion to the expected type
        if header_key == ID_HEADER_KEY:
            metadata_kwargs[metadata_field] = UUID(header_value)
        elif header_key == TIME_HEADER_KEY:
            metadata_kwargs[metadata_field] = datetime.fromisoformat(header_value)
        elif header_key == SOURCELIB_HEADER_KEY:
            metadata_kwargs[metadata_field] = _sourcelib_str_to_tuple(header_value)
        else:
            # these are all string values and don't need any conversion step
            metadata_kwargs[metadata_field] = header_value
    return oed.EventsMetadata(**metadata_kwargs)


def _get_headers_from_metadata(event_metadata: oed.EventsMetadata):
    """
    Create a dictionary of CloudEvent-compliant Kafka headers from an EventsMetadata object.

    This method assumes the EventMetadata object was the one sent with the event data to the original signal handler.

    Arguments:
        event_metadata: An EventsMetadata object sent by an OpenEdxPublicSignal

    Returns:
        A dictionary of headers
    """
    # Dictionary (or list of key/value tuples) where keys are strings and values are binary.
    # CloudEvents specifies using UTF-8; that should be the default, but let's make it explicit.
    return {
        # The way EventMetadata is initialized none of these should ever be null.
        # If it is we want the error to be raised.
        EVENT_TYPE_HEADER_KEY: event_metadata.event_type.encode("utf-8"),
        ID_HEADER_KEY: str(event_metadata.id).encode("utf-8"),
        SOURCE_HEADER_KEY: event_metadata.source.encode("utf-8"),
        SOURCEHOST_HEADER_KEY: event_metadata.sourcehost.encode("utf-8"),
        TIME_HEADER_KEY: event_metadata.time.isoformat().encode("utf-8"),
        MINORVERSION_HEADER_KEY: str(event_metadata.minorversion).encode("utf-8"),
        SOURCELIB_HEADER_KEY: _sourcelib_tuple_to_str(event_metadata.sourcelib).encode("utf-8"),

        # Always 1.0. See "Fields" in OEP-41
        SPEC_VERSION_HEADER_KEY: b'1.0',
        CONTENT_TYPE_HEADER_KEY: b'application/avro',
        DATA_CONTENT_TYPE_HEADER_KEY: b'application/avro',
    }
