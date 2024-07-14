import asyncio
import time
import logging
from typing import Callable, Dict, Tuple

from bacpypes3.apdu import SubscribeCOVRequest, ConfirmedCOVNotificationRequest
from bacpypes3.primitivedata import ObjectIdentifier, PropertyIdentifier
from bacpypes3.errors import DecodingError, ExecutionError
from BACeyesApp import BACeyesApp
from bacpypes3.app import ApplicationServiceElement

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)

class BACeyesSubscription:
    """Represents a BACnet COV subscription."""

    def __init__(
        self,
        device_id: int,
        obj_id: ObjectIdentifier,
        prop_id: PropertyIdentifier,
        confirmed_notifications: bool = True,
        lifetime_seconds: int = None,
        cov_increment: float = None,
        cov_increment_percentage: float = None,
    ):
        self.device_id = device_id
        self.obj_id = obj_id
        self.prop_id = prop_id
        self.confirmed_notifications = confirmed_notifications
        self.lifetime_seconds = lifetime_seconds
        self.cov_increment = cov_increment
        self.cov_increment_percentage = cov_increment_percentage
        self.active = False
        self.last_renewed_time = None
        self.subscription_id = None  # Add subscription_id attribute

    async def add(self, app: BACeyesApp, timeout: int = 5):
        """Add the subscription to the BACnet network.

        Args:
            app (BACeyesApp): The BACnet application instance.
            timeout (int, optional): Timeout in seconds for the subscription request. Defaults to 5.
        """

        try:
            device_address = app.device_info_cache.get_device_address(self.device_id)
            if not device_address:
                raise ValueError(f"Device {self.device_id} not found")

            subscription_request = SubscribeCOVRequest(
                subscriberProcessIdentifier=app.this_application.localDevice.objectIdentifier[1],
                monitoredObjectIdentifier=self.obj_id,
                issueConfirmedNotifications=self.confirmed_notifications,
                lifetimeInSeconds=self.lifetime_seconds,
                destination=device_address,
            )

            if self.cov_increment is not None:
                subscription_request.covIncrement = self.cov_increment
            elif self.cov_increment_percentage is not None:
                subscription_request.covIncrementPercentage = self.cov_increment_percentage

            response = await asyncio.wait_for(app.request(subscription_request), timeout=timeout)  # Await the request coroutine


            self.active = True
            self.last_renewed_time = time.time()
            _log.info(
                f"COV subscription added: {self.obj_id}.{self.prop_id} on device {self.device_id}"
            )
        except (Exception, asyncio.TimeoutError) as e:  # Catch TimeoutError from asyncio.wait_for
            _log.error(
                f"Error adding subscription: {self.obj_id}.{self.prop_id} on device {self.device_id} - {e}"
            )

    def cancel(self, bacnet_cov_instance):  # Added parameter to access BACnetCOV instance
        """Cancel the subscription."""
        if (self.device_id, self.obj_id, self.prop_id) in bacnet_cov_instance.subscriptions:
            bacnet_cov_instance.subscriptions.pop((self.device_id, self.obj_id, self.prop_id))
        if (self.device_id, self.obj_id, self.prop_id) in bacnet_cov_instance.callbacks:
            bacnet_cov_instance.callbacks.pop((self.device_id, self.obj_id, self.prop_id))
        self.active = False
        _log.info(f"Subscription canceled: {self.obj_id}.{self.prop_id} on device {self.device_id}")

    async def renew_subscription(self, app: BACeyesApp, timeout: int = 5):
        """Renews the COV subscription, handling potential errors gracefully."""

        device_address = app.device_info_cache.get_device_address(self.device_id)

        subscription_info = f"{self.obj_id}.{self.prop_id}"

        if not self.active:
            _log.warning(f"Attempting to renew inactive subscription: {subscription_info}")
            return

        _log.info(f"Renewing COV subscription: {subscription_info}")

        # Create a new SubscribeCOVRequest to renew the subscription
        subscription_request = SubscribeCOVRequest(
            subscriberProcessIdentifier=app.this_application.localDevice.objectIdentifier[1],
            monitoredObjectIdentifier=self.obj_id,
            issueConfirmedNotifications=self.confirmed_notifications,
            lifetimeInSeconds=self.lifetime_seconds,
            destination=device_address
        )

        # Create a new subscription object with same parameters as original except for start time
        new_subscription = BACeyesSubscription(
            self.device_id,
            self.obj_id,
            self.prop_id,
            self.confirmed_notifications,
            self.lifetime_seconds,
            self.cov_increment,
            self.cov_increment_percentage,
        )
        # Store the new subscription and callback
        app.bacnet_cov.subscriptions[(self.device_id, self.obj_id, self.prop_id)] = new_subscription

        try:
            # Use the new method add to renew the subscription
            await asyncio.wait_for(new_subscription.add(app), timeout=timeout)
        except (Exception, asyncio.TimeoutError) as e:
            _log.error(f"Error renewing subscription: {self.obj_id}.{self.prop_id} on device {self.device_id} - {e}")
            # Handle error as appropriate (e.g., retry, remove subscription, alert user)


class BACeyesCOV(ApplicationServiceElement):  # Inherit from ApplicationServiceElement
    def __init__(self, app: BACeyesApp):
        super().__init__()  # Call the superclass constructor

        def __init__(self, baceyes_app: BACeyesApp):
            self.baceyes_app = baceyes_app
            self.devices: Dict[ObjectIdentifier, BACeyesDevices] = {}
            self.device_address_map: Dict[ObjectIdentifier, Address] = {}

        self.subscriptions: Dict[Tuple[int, ObjectIdentifier, PropertyIdentifier], BACeyesSubscription] = {}
        self.callbacks: Dict[Tuple[int, ObjectIdentifier, PropertyIdentifier], Callable] = {}

    async def subscribe(self, device_id: int, obj_id: ObjectIdentifier, prop_id: PropertyIdentifier, callback: Callable,
                        confirmed_notifications: bool = True, lifetime: int = None, cov_increment: float = None,
                        cov_increment_percentage: float = None):
        """Subscribes to COV for a specified property."""
        _log.info(f"Subscribing to COV for {obj_id}.{prop_id} on device {device_id}")

        subscription = BACeyesSubscription(
            device_id, obj_id, prop_id, confirmed_notifications, lifetime, cov_increment, cov_increment_percentage
        )
        self.subscriptions[(device_id, obj_id, prop_id)] = subscription
        self.callbacks[(device_id, obj_id, prop_id)] = callback

        # Create a new SubscribeCOVRequest to subscribe
        device_address = self.bacnet_app.device_info_cache.get_device_address(device_id)
        if not device_address:
            _log.error(f"Device {device_id} not found")
            return

        subscription_request = SubscribeCOVRequest(
            subscriberProcessIdentifier=self.bacnet_app.this_application.localDevice.objectIdentifier[1],
            monitoredObjectIdentifier=obj_id,
            issueConfirmedNotifications=confirmed_notifications,
            lifetimeInSeconds=lifetime,
            destination=device_address
        )

        if cov_increment is not None:
            subscription_request.covIncrement = cov_increment
        elif cov_increment_percentage is not None:
            subscription_request.covIncrementPercentage = cov_increment_percentage

        await subscription.add(self.bacnet_app)
        await self._process_subscription(subscription,
                                         subscription_request)  # Start the subscription process immediately

    async def handle_cov_notification(self, pdu: ConfirmedCOVNotificationRequest):
        """Handles incoming COV notifications."""

        key = (
            pdu.initiatingDeviceIdentifier[1],
            pdu.monitoredObjectIdentifier,
            pdu.monitoredPropertyIdentifier,
        )

        try:
            callback = self.callbacks.get(key)
            if callback:
                _log.info(
                    f"COV notification for {key[1]}.{key[2]} on device {key[0]}: {pdu.listOfValues}"
                )
                await callback(pdu.listOfValues)
            else:
                _log.warning(
                    f"Received COV notification for unsubscribed property {key[1]}.{key[2]} on device {key[0]}"
                )
        except Exception as e:
            _log.error(f"Error handling COV notification: {e}")

    async def _process_subscription(self, subscription: BACeyesSubscription, cov_request: SubscribeCOVRequest):
        """Handles the async context for COV subscriptions and renewals."""

        # Use BACpypes3's request mechanism for handling COV subscriptions
        response = await self.bacnet_app.request(cov_request)

        # Handle unsuccessful subscription attempts
        if not isinstance(response, SubscribeCOVRequest):
            _log.error(
                f"Failed to subscribe to COV for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}"
            )
            return

        # Handle COV notifications until cancelled
        while True:
            try:
                notification = await self.bacnet_app.response_queue.get()
                if (
                    isinstance(notification, ConfirmedCOVNotificationRequest)
                    and notification.monitoredObjectIdentifier == subscription.obj_id
                    and notification.monitoredPropertyIdentifier == subscription.prop_id
                    and notification.subscriberProcessIdentifier
                    == cov_request.subscriberProcessIdentifier
                ):
                    property_identifier = notification.monitoredPropertyIdentifier
                    property_value = notification.listOfValues
                    _log.info(
                        f"Received COV notification: {subscription.obj_id}.{property_identifier} = {property_value}"
                    )
                    callback = self.callbacks.get(
                        (
                            subscription.device_id,
                            subscription.obj_id,
                            subscription.prop_id,
                        )
                    )
                    if callback:
                        await callback(property_value)
            except asyncio.CancelledError:
                _log.info(
                    f"Subscription to {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id} cancelled"
                )
                break
        _log.info(
            f"Subscription for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id} ended."
        )
    async def manage_subscriptions(self):
        """Manages active subscriptions, including renewals."""

        _log.debug("Starting subscription management task.")

        while True:
            try:
                _log.debug("Checking subscriptions for renewal...")
                for subscription in list(self.subscriptions.values()):  # Iterate over a copy of the subscriptions
                    if subscription.active and subscription.lifetime_seconds is not None:
                        remaining_lifetime = subscription.lifetime_seconds - (
                                    time.time() - subscription.last_renewed_time)
                        if remaining_lifetime < 60:  # Renew a minute before expiration
                            _log.info(
                                f"Renewing subscription: {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}"
                            )
                            await self.subscribe(  # Renew by re-subscribing
                                subscription.device_id,
                                subscription.obj_id,
                                subscription.prop_id,
                                self.callbacks[(subscription.device_id, subscription.obj_id, subscription.prop_id)],
                                # Use existing callback
                                subscription.confirmed_notifications,
                                subscription.lifetime_seconds
                            )
            except Exception as e:
                _log.error(f"Error managing subscriptions: {e}")

            await asyncio.sleep(60)  # Check for renewal every minute (adjust as needed)

    async def unsubscribe(self, device_id: int, obj_id: ObjectIdentifier, prop_id: PropertyIdentifier):
        """Unsubscribes from COV for a specified property."""

        _log.info(f"Unsubscribing from COV for {obj_id}.{prop_id} on device {device_id}")

        key = (device_id, obj_id, prop_id)
        subscription = self.subscriptions.pop(key, None)
        self.callbacks.pop(key, None)

        if subscription:
            subscription.cancel(self)

            if subscription.subscription_id is not None:  # Use subscription_id
                try:
                    device_address = self.bacnet_app.device_info_cache.get_device_address(device_id)
                    if not device_address:
                        raise ValueError(f"Device {device_id} not found")

                    request = SubscribeCOVRequest(
                        subscriberProcessIdentifier=subscription.subscription_id,  # Use subscription_id
                        monitoredObjectIdentifier=obj_id,
                        destination=device_address,
                        lifetime=0,  # Setting lifetime to 0 unsubscribes
                    )
                    await self.bacnet_app.request(request)
                except (ValueError, DecodingError, ExecutionError, TimeoutError) as e:
                    _log.error(f"Error unsubscribing from COV for {obj_id}.{prop_id} on device {device_id}: {e}")
