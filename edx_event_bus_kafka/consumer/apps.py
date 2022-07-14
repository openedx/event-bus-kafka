"""App for consuming Kafka events. Comprises a management command for listening to a topic and supporting methods.
Likely temporary."""
from django.apps import AppConfig
from edx_django_utils.plugins.constants import PluginSettings


class KafkaConsumerApp(AppConfig):
    """
    Configuration for the KafkaConsumer Django application.
    """

    name = 'edx_arch_experiments.kafka_consumer'

    plugin_app = {
        PluginSettings.CONFIG: {
            'lms.djangoapp': {
                'common': {
                    PluginSettings.RELATIVE_PATH: 'settings.common',
                },
                'devstack': {
                    PluginSettings.RELATIVE_PATH: 'settings.devstack',
                },
                'production': {
                    PluginSettings.RELATIVE_PATH: 'settings.production',
                },
            }
        }
    }
    from .signals import receivers  # pylint: disable=import-outside-toplevel
