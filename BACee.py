# BACee

# Modular BACnet Communication for BBMD Devices for mid to lrg networks: uses bacpypes
# this code provides a framework for BACnet communication and COV subscription management. 
# It allows you to discover BBMDs, monitor BACnet objects for changes, and receive 
# notifications when property values change...

# MIT License - 2024 by: eex0

import bacpypes
import json
import logging
import threading
import time

from bacpypes.object import validate_object_id
from bacpypes.pdu import PDU
from bacpypes.apdu import (
    WhoIsRequest, IAmRequest, SubscribeCOVRequest,
    ConfirmedCOVNotificationRequest, SimpleAckPDU,
    ReadPropertyRequest, WritePropertyRequest, ReadPropertyACK,
    Error, RejectPDU, AbortPDU
)
from bacpypes.primitivedata import Unsigned
from bacpypes.app import BIPSimpleApplication
from bacpypes.service.cov import ChangeOfValueServices
from bacpypes.errors import DecodingError, CommunicationError
from bacpypes.constructeddata import ArrayOf
from bacpypes.core import deferred, run
from bacpypes.iocb import IOCB
from bacpypes.local.device import LocalDeviceObject
from bacpypes.object import get_object_class, get_datatype, PropertyIdentifier

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global dictionary for COV subscriptions
subscriptions = {}


class CustomCOVApplication(BIPSimpleApplication, ChangeOfValueServices):
    def __init__(self, device_name, device_id, local_address, bbmd_address=None):
        # Create a local device object
        self.this_device = LocalDeviceObject(
            objectName=device_name,
            objectIdentifier=('device', device_id)
        )

        # Initialize BACnet application
        BIPSimpleApplication.__init__(self, self.this_device, local_address, bbmd_address)
        ChangeOfValueServices.__init__(self)

        self.object_values = {}
        self.cov_subscriptions = {}
        self.discoveredDevices = []
        self._value_check_timer = None

    def do_WhoIsRequest(self, apdu):
        """Respond to Who-Is requests to support BBMD and device discovery."""
        logger.info(f"Received Who-Is Request from {apdu.pduSource}")

        if self.local_address is None:  # Only respond if we are not a BBMD
            logger.info(f"Sending I-Am Request for device: {self.this_device.objectName[0].value}")
            self.response(IAmRequest())

    def do_IAmRequest(self, apdu):
        """Process I-Am requests to track discovered devices."""
        device_id = apdu.iAmDeviceIdentifier[1]  # Assuming device identifier is the second octet
        device_address = apdu.pduSource
        logger.info(f"Received I-Am Request from device {device_id} at {device_address}")
        self.discoveredDevices.append(apdu)

    def do_SubscribeCOVRequest(self, apdu):
        """Handles SubscribeCOVRequests from clients."""
        logger.info("Received SubscribeCOVRequest")
        obj_id = apdu.monitoredObjectIdentifier
        confirmed = apdu.issueConfirmedNotifications
        lifetime = apdu.lifetime

        if obj_id is not None:
            lifetime_seconds = lifetime.intValueSeconds() if lifetime else None  # Get lifetime in seconds

            subscription = (confirmed, lifetime_seconds, apdu.pduSource, None, apdu.subscriberProcessIdentifier)
            self.cov_subscriptions[obj_id] = self.cov_subscriptions.get(obj_id, []) + [subscription]

            self.send_ack(SimpleAckPDU(context=apdu))  # Acknowledge the subscription

            # Schedule the subscription for renewal if a lifetime is specified
            if lifetime_seconds is not None:
                self.schedule_subscription_renewal(obj_id, lifetime_seconds, apdu.subscriberProcessIdentifier)

            # Start the process to track object values (if not already running)
            if not hasattr(self, '_value_check_timer') or not self._value_check_timer.is_alive():
                self._value_check_timer = threading.Timer(5, self.check_object_values)
                self._value_check_timer.start()
        else:
            self.send_error(Error(errorClass='object', errorCode='unknownObject', context=apdu))
    
    def schedule_subscription_renewal(self, obj_id, lifetime_seconds, subscriberProcessIdentifier):
        """Schedule renewal of a COV subscription."""
        timer = threading.Timer(lifetime_seconds, self._renew_subscription, args=[obj_id, subscriberProcessIdentifier])
        # Update the subscription with the timer and subscriberProcessIdentifier
        for i, sub in enumerate(self.cov_subscriptions[obj_id]):
            if sub[4] == subscriberProcessIdentifier:  # Use subscriberProcessIdentifier for identification
                self.cov_subscriptions[obj_id][i] = (*sub[:3], timer, subscriberProcessIdentifier)
                break
        timer.start()
        logger.info(f"Scheduled COV subscription renewal for {obj_id} in {lifetime_seconds} seconds")

    def _renew_subscription(self, obj_id, subscriberProcessIdentifier):
        """Renew a COV subscription."""
        logger.info(f"Renewing subscription for object: {obj_id}")
        subscriptions = self.cov_subscriptions.get(obj_id)
        if subscriptions:
            for i, subscription in enumerate(subscriptions):
                if subscription[4] == subscriberProcessIdentifier:
                    confirmed_notifications, lifetime_seconds, pduSource, _, _ = subscription
                    self.schedule_subscription_renewal(obj_id, lifetime_seconds, subscriberProcessIdentifier)
                    # Send a notification upon renewal (if confirmed notifications are requested)
                    if confirmed_notifications:
                        self.send_cov_notification(
                            obj_id,
                            self.object_values[obj_id],
                            subscriberProcessId=subscriberProcessIdentifier,
                            destination=pduSource
                        )  
                    break  # Only renew the matching subscription
        else:
            logger.warning(f"Subscription for object {obj_id} not found. Renewal failed.")

    def do_UnsubscribeCOVRequest(self, apdu):
        """Unsubscribe from COV notifications."""
        logger.info(f"Received UnsubscribeCOVRequest from {apdu.pduSource}")
        subscriber_process_id = apdu.subscriberProcessIdentifier
        obj_id = apdu.monitoredObjectIdentifier

        if obj_id in self.cov_subscriptions:
            subscriptions_for_object = self.cov_subscriptions[obj_id]
            self.cov_subscriptions[obj_id] = [sub for sub in subscriptions_for_object if sub[4] != subscriber_process_id]
        else:
            logger.warning(f"No subscription found for object {obj_id} with subscriber process ID {subscriber_process_id}")

        if obj_id not in self.cov_subscriptions or not self.cov_subscriptions[obj_id]:  # If no more subscriptions
            if hasattr(self, '_value_check_timer') and self._value_check_timer.is_alive():
                self._value_check_timer.cancel()
                del self._value_check_timer
                logger.info(f"Stopped checking object values for {obj_id} as there are no more subscriptions")

        self.send_ack(SimpleAckPDU(context=apdu))  # Acknowledge the unsubscription

    def check_object_values(self):
        """Periodically check object values for changes and send notifications."""
        threading.Timer(5, self.check_object_values).start()  # Reschedule the check

        for obj_id, subscriptions in self.cov_subscriptions.items():
            try:
                result = self.read(f'{obj_id}')
                if result and isinstance(result, ReadPropertyACK):
                    # Assuming the first element of value is what we want to monitor
                    present_value = result.propertyValue[0].value[0]

                    if obj_id not in self.object_values or present_value != self.object_values[obj_id]:
                        self.object_values[obj_id] = present_value

                        # Send notifications only to subscribers who requested confirmed notifications
                        for subscription in subscriptions:
                            confirmed_notifications, _, pduSource, _, subscriberProcessId = subscription
                            if confirmed_notifications:
                                self.send_cov_notification(
                                    obj_id,
                                    present_value,
                                    subscriberProcessId=subscriberProcessId,
                                    destination=pduSource
                                )
            except Exception as error:
                logger.error(f"Error checking object value for {obj_id}: {error}")

    def send_cov_notification(self, obj_id, value, subscriberProcessId, destination):
        """Send a confirmed COV notification."""
        # Construct the notification APDU
        apdu = ConfirmedCOVNotificationRequest(
            subscriberProcessIdentifier=subscriberProcessId,
            initiatingDeviceIdentifier=self.this_device.objectIdentifier,
            monitoredObjectIdentifier=obj_id,
            timeRemaining=Unsigned(60)  # Assuming 1 minute lifetime for the notification
        )
        # Add the property value to the APDU
        datatype = get_datatype(obj_id.objectType, "presentValue")
        apdu.listOfValues = ArrayOf(datatype)
        apdu.listOfValues.append(bacpypes.primitivedata.encode_application_data(value))

        # Send the APDU
        logger.info(f"Sending ConfirmedCOVNotificationRequest to {destination} for {obj_id}")
        self.request(apdu, destination)

class BACnetClient:
    def __init__(self, bbmd_address, device_name='Custom-Client', device_id=1234):
        self.bbmd_address = bbmd_address
        self.app = CustomCOVApplication(device_name, device_id, bacpypes.local.BIPLocalAddress(), bbmd_address)
        self.logger = logging.getLogger(__name__)
        self.devices = []

    def discover_devices(self) -> list[dict]:
        """Discover BACnet devices through the BBMD."""
        logger.info("Discovering devices...")
        who_is = WhoIsRequest()
        self.app.request(who_is)
        # Add a small delay to allow for responses
        deferred(self._get_discovered_devices, 1.0)  
        return self.devices  # Return the populated list of devices

    def _get_discovered_devices(self):
        """Store Discovered devices in a list"""
        self.devices = []  # Reset the devices list before re-populating
        for device in self.app.discoveredDevices:
            device_dict = {
                'device_id': device.iAmDeviceIdentifier,
                'device_address': device.pduSource,
                'device_name': device.objectName,
                'max_apdu_length_accepted': device.maxApduLengthAccepted,
                'segmentation_supported': device.segmentationSupported,
                'vendor_id': device.vendorID,
            }
            self.devices.append(device_dict)

    def read_property(self, object_identifier, property_id):
        """Reads a property from a BACnet object."""
        try:
            iocb = self.app.read(
                f'{object_identifier}/{property_id}'
            )
            if iocb.ioResponse:
                apdu = iocb.ioResponse
                if not isinstance(apdu, ReadPropertyACK):
                    raise ValueError("ReadProperty did not succeed, got: {apdu}")
                # Here you extract the value from the APDU
                return apdu.propertyValue[0].value[0]
        except (CommunicationError, BACnetError) as e:
            self.logger.error(f"Error reading property: {e}")

    def write_property(self, object_identifier, property_id, value):
        """Writes a property to a BACnet object."""
        try:
            iocb = self.app.write(
                f'{object_identifier}/{property_id}:{value}'
            )
            # check for success
            if iocb.ioError:
                self.logger.error(f"Error writing property: {iocb.ioError}")
        except (CommunicationError, BACnetError) as e:
            self.logger.error(f"Error writing property: {e}")

    def subscribe_to_changes(self, object_identifier, property_id, callback=None, confirmed_notifications=True, lifetime=None):
        """Subscribe to COV notifications for the specified object and property."""
        try:
            self.app.do_SubscribeCOVRequest(
                SubscribeCOVRequest(
                    monitoredObjectIdentifier=object_identifier,
                    issueConfirmedNotifications=confirmed_notifications,
                    subscriberProcessIdentifier=0,  # Assuming only one subscriber for now,
                    lifetime=Unsigned(lifetime) if lifetime else None
                )
            )
            if callback:
                subscriptions[(object_identifier, property_id)] = callback
                logging.info(f"Subscribed to changes for object: {object_identifier}, property: {property_id}")
        except (CommunicationError, BACnetError) as e:
            logger.error(f"Error subscribing to changes: {e}")

# --- Main Function with Tests ---

def main():
    # Replace with the actual BBMD address and port
    bbmd_address = ("192.168.1.100", 47808)

    # Initialize the client
    client = BACnetClient(bbmd_address, device_name="BACnetCOVClient", device_id=123) 

    # Discover devices and BBMDs
    client.app.discover_remote_devices()
    # Add a delay to wait for responses before proceeding
    time.sleep(2)  

    # Testing and Demo
    if client.app.discoveredDevices:
        logging.info("BBMD and Device Discovery Test: PASS")

        # Example: Subscribe to COV for Present Value on AI1 for each discovered device
        for device in client.app.discoveredDevices:
            device_id = device.iAmDeviceIdentifier
            client.subscribe_to_changes(device_id, PropertyIdentifier.presentValue, cov_callback, lifetime=60)  # Subscribe for 60 seconds

        while True:
            run()  # Keep the BACnet stack running
    else:
        logging.error("BBMD and Device Discovery Test: FAIL - No devices or BBMDs found.")


# Callback function for COV notifications
def cov_callback(obj_id, prop_id, value):
    logging.info(f"COV Notification: Object {obj_id}, Property {prop_id}, Value: {value}")

if __name__ == "__main__":
    main()

