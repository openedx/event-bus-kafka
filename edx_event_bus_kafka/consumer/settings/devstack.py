"""Devstack settings values."""

import os
from os.path import abspath, dirname, join


def plugin_settings(settings):
    """ Override/set kafka_consumer devstack settings"""
    settings.KAFKA_BOOTSTRAP_SERVER = "edx.devstack.kafka:29092"
    settings.SCHEMA_REGISTRY_URL = "http://edx.devstack.schema-registry:8081"
    settings.LICENSE_EVENT_TOPIC_NAME = "license-event-dev"
    if os.path.isfile(join(dirname(abspath(__file__)), 'private.py')):
        from .private import plugin_settings_override  # pylint: disable=import-outside-toplevel,import-error
        plugin_settings_override(settings)
