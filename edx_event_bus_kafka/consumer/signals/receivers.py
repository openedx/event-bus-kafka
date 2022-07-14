"""Signal receivers for the event bus"""

import logging

from django.dispatch import receiver
from openedx_events.enterprise.data import SubscriptionLicenseData
from openedx_events.enterprise.signals import SUBSCRIPTION_LICENSE_MODIFIED

logger = logging.getLogger(__name__)


@receiver(SUBSCRIPTION_LICENSE_MODIFIED)
def log_event_from_event_bus(**kwargs):  # pragma: no cover
    """
    Log event received and transmitted from event bus consumer

    :param kwargs: event data sent by signal
    """
    license_data = kwargs.get('license', None)
    if not license_data or not isinstance(license_data, SubscriptionLicenseData):
        logger.error("Received null or incorrect data from SUBSCRIPTION_LICENSE_MODIFIED")
        return
    logger.info(f"Received SUBSCRIPTION_LICENSE_MODIFIED signal with license_data"
                f" with SubscriptionLicenseData {license_data}")
