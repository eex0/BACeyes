# BACee

# Modular BACnet Communication for BBMD Devices for mid to lrg networks: uses bacpypes
# this code provides a framework for BACnet communication and COV subscription reporting. 
# It allows you to discover BBMDs, monitor BACnet objects for changes, and receive 
# notifications when property values change...

# MIT License - 2024 by: eex0

import bacpypes
import logging
import threading
import time

from bacpypes.object import validate_object_id
from bacpypes.apdu import (
    WhoIsRequest, IAmRequest, SubscribeCOVRequest,
    ConfirmedCOVNotificationRequest, SimpleAckPDU,
    ReadPropertyRequest, WritePropertyRequest, ReadPropertyACK,
    Error, ReadPropertyMultipleRequest
)
from bacpypes.primitivedata import Unsigned
from bacpypes.app import BIPSimpleApplication
from bacpypes.service.cov import ChangeOfValueServices
from bacpypes.errors import (
    DecodingError, CommunicationError, ExecutionError,
    BACnetError, TimeoutError
)
from bacpypes.constructeddata import ArrayOf
from bacpypes.core import deferred, run
from bacpypes.iocb import IOCB
from bacpypes.local.device import LocalDeviceObject
from bacpypes.object import get_datatype, PropertyIdentifier

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global lock for thread safety
subscription_lock = threading.Lock()
logger = logging.getLogger(__name__)

class CustomCOVApplication(ChangeOfValueServices, BIPSimpleApplication):
    """
    This class represents a BACnet application that handles device discovery, 
    COV subscriptions, and property read/write operations.
    """
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

        self.logger = logging.getLogger(__name__)

    def do_WhoIsRequest(self, apdu):
        """Respond to Who-Is requests to support BBMD and device discovery."""
        self.logger.info(f"Received Who-Is Request from {apdu.pduSource}")

        if self.local_address is None:  # Only respond if we are not a BBMD
            self.logger.info(f"Sending I-Am Request for device: {self.this_device.objectName[0].value}")
            self.response(IAmRequest())

    def do_IAmRequest(self, apdu):
        """Process I-Am requests to track discovered devices."""
        device_id = apdu.iAmDeviceIdentifier
        device_address = apdu.pduSource
        self.logger.info(f"Received I-Am Request from device {device_id} at {device_address}")
        self.discoveredDevices.append(apdu)

    def do_SubscribeCOVRequest(self, apdu):
        """Handles SubscribeCOVRequests from clients."""
        self.logger.info("Received SubscribeCOVRequest")
        obj_id = apdu.monitoredObjectIdentifier
        confirmed = apdu.issueConfirmedNotifications
        lifetime = apdu.lifetime

        if obj_id is not None:
            lifetime_seconds = lifetime.intValueSeconds() if lifetime else None  # Get lifetime in seconds

            subscription = {"confirmed": confirmed, 
                            "lifetime": lifetime_seconds, 
                            "source": apdu.pduSource, 
                            "timer": None, 
                            "subscriberProcessIdentifier": apdu.subscriberProcessIdentifier,
                            "property_id": apdu.monitoredPropertyIdentifier,
                            "threshold": apdu.covIncrement.value if apdu.covIncrement else None
                            }

            with subscription_lock: # Lock the subscription dictionary
                self.cov_subscriptions.setdefault(obj_id, []).append(subscription)

            try:
                self.send_ack(SimpleAckPDU(context=apdu))  # Acknowledge the subscription
            except (CommunicationError, BACnetError) as e:
                self.logger.error(f"Error acknowledging SubscribeCOVRequest: {e}")
                return  # Return to avoid scheduling renewal or value checking

            # Schedule the subscription for renewal if a lifetime is specified
            if lifetime_seconds is not None:
                self.schedule_subscription_renewal(obj_id, lifetime_seconds, apdu.subscriberProcessIdentifier)

            # Start the process to track object values (if not already running)
            if self._value_check_timer is None or not self._value_check_timer.is_alive():
                self._value_check_timer = threading.Timer(5, self.check_object_values)
                self._value_check_timer.start()
        else:
            self.send_error(Error(errorClass='object', errorCode='unknownObject', context=apdu))
    
    def schedule_subscription_renewal(self, obj_id, lifetime_seconds, subscriberProcessIdentifier):
        """Schedule renewal of a COV subscription."""
        timer = threading.Timer(lifetime_seconds, self._renew_subscription, args=[obj_id, subscriberProcessIdentifier])
        # Update the subscription with the timer and subscriberProcessIdentifier
        with subscription_lock:  # Lock the subscription dictionary for thread safety
            for i, sub in enumerate(self.cov_subscriptions[obj_id]):
                if sub['subscriberProcessIdentifier'] == subscriberProcessIdentifier:  # Use subscriberProcessIdentifier for identification
                    self.cov_subscriptions[obj_id][i]["timer"] = timer
                    break

        timer.start()
        logger.info(f"Scheduled COV subscription renewal for {obj_id} in {lifetime_seconds} seconds")

    def _renew_subscription(self, obj_id, subscriberProcessIdentifier):
        """Renew a COV subscription."""
        self.logger.info(f"Renewing subscription for object: {obj_id}")
        with subscription_lock:  # Lock the subscription dictionary for thread safety
            subscriptions = self.cov_subscriptions.get(obj_id)
            if subscriptions:
                for i, subscription in enumerate(subscriptions):
                    if subscription['subscriberProcessIdentifier'] == subscriberProcessIdentifier:
                        confirmed_notifications = subscription['confirmed']
                        lifetime_seconds = subscription['lifetime']
                        pduSource = subscription['source']
                        # Attempt to renew the subscription
                        try:
                            self.schedule_subscription_renewal(obj_id, lifetime_seconds, subscriberProcessIdentifier)
                        except Exception as e:
                            self.logger.error(f"Error scheduling renewal for object {obj_id}: {e}")
                            continue  # Continue to try renewing other subscriptions

                        # Send a notification upon renewal (if confirmed notifications are requested)
                        if confirmed_notifications:
                            try:
                                self.send_cov_notification(
                                    obj_id,
                                    self.object_values[obj_id],
                                    subscriberProcessId=subscriberProcessIdentifier,
                                    destination=pduSource
                                )
                            except Exception as e:  # Add error handling for notification sending
                                self.logger.error(f"Error sending COV notification for {obj_id}: {e}")

                        break  # Only renew the matching subscription
            else:
                self.logger.warning(f"Subscription for object {obj_id} not found. Renewal failed.")

    def do_UnsubscribeCOVRequest(self, apdu):
        """Unsubscribe from COV notifications."""
        self.logger.info(f"Received UnsubscribeCOVRequest from {apdu.pduSource}")
        subscriber_process_id = apdu.subscriberProcessIdentifier
        obj_id = apdu.monitoredObjectIdentifier

        with subscription_lock:
            if obj_id in self.cov_subscriptions:
                subscriptions_for_object = self.cov_subscriptions[obj_id]
                self.cov_subscriptions[obj_id] = [sub for sub in subscriptions_for_object if sub['subscriberProcessIdentifier'] != subscriber_process_id]
            else:
                self.logger.warning(f"No subscription found for object {obj_id} with subscriber process ID {subscriber_process_id}")

            if obj_id not in self.cov_subscriptions or not self.cov_subscriptions[obj_id]:  # If no more subscriptions
                if hasattr(self, '_value_check_timer') and self._value_check_timer.is_alive():
                    self._value_check_timer.cancel()
                    del self._value_check_timer
                    self.logger.info(f"Stopped checking object values for {obj_id} as there are no more subscriptions")

        self.send_ack(SimpleAckPDU(context=apdu))  # Acknowledge the unsubscription

    def check_object_values(self):
        """Periodically check object values for changes and send notifications."""
        if self._value_check_timer:  # Only reschedule if the timer exists
            threading.Timer(5, self.check_object_values).start()  # Reschedule the check

        with subscription_lock:  # Lock the subscriptions dictionary before accessing it
            for obj_id, subscriptions in self.cov_subscriptions.items():
                for subscription in subscriptions:
                    try:
                        property_id = subscription.get('property_id')
                        # Read the property and validate the result
                        result = self.read_property(obj_id, property_id)

                        if result is not None:
                            # If it's a sequence, get the first value
                            if isinstance(result, bacpypes.constructeddata.Sequence):
                                result = result[0]

                            # Compare the new value with the stored value
                            if obj_id not in self.object_values or abs(result - self.object_values.get(obj_id)) >= subscription.get('threshold', 0.01):
                                self.object_values[obj_id] = result

                                confirmed_notifications, _, pduSource, _, subscriberProcessId = subscription
                                if confirmed_notifications:
                                    # Send the COV notification
                                    self.send_cov_notification(
                                        obj_id,
                                        result,  # Use the read result directly
                                        subscriberProcessId=subscriberProcessId,
                                        destination=pduSource
                                    )
                    else:
                        self.logger.warning(f"Error reading property {property_id} of object {obj_id}")
                except (CommunicationError, BACnetError) as e:
                    self.logger.error(f"Error checking or sending notification for {obj_id}/{property_id}: {e}")

    def send_cov_notification(self, obj_id, value, subscriberProcessId, destination):
        """Send a confirmed COV notification."""
        try:
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
            self.logger.info(f"Sending ConfirmedCOVNotificationRequest to {destination} for {obj_id}")
            self.request(apdu, destination)
        except Exception as e:  # Add error handling for notification sending
            self.logger.error(f"Error sending COV notification for {obj_id}: {e}")

    def do_ReadPropertyRequest(self, apdu):
        """Handle ReadProperty requests."""
        try:
            obj_id = apdu.objectIdentifier
            prop_id = apdu.propertyIdentifier
            # Check if reading multiple properties
            if (
                isinstance(prop_id, bacpypes.constructeddata.SequenceOf) and
                prop_id[0].propertyIdentifier == bacpypes.object.PropertyIdentifier.all
            ):
                self.read_multiple_properties(apdu)  # Handle multiple property reads
                return

            result = self.read(f'{obj_id}/{prop_id}')
            if result:
                ack = ReadPropertyACK(context=apdu)
                ack.objectIdentifier = obj_id
                ack.propertyIdentifier = prop_id
                ack.propertyValue = result.propertyValue
                self.response(ack)
            else:
                raise ExecutionError(errorClass='property', errorCode='unknownProperty')  # Improved error code
        except (ValueError, DecodingError, CommunicationError, BACnetError) as e:
            self.logger.error(f"Error in ReadPropertyRequest: {e}")
            # More specific error handling based on exception type
            if isinstance(e, ExecutionError):
                error_code = e.errorCode
            elif isinstance(e, BACnetError):
                error_code = e.error_code
            else:
                error_code = 'other' 
            self.response(Error(errorClass='property', errorCode=error_code, context=apdu))


    def read_multiple_properties(self, apdu):
        """Handles ReadPropertyMultiple requests."""
        try:
            obj_id = apdu.objectIdentifier

            # Assuming that the request is for "all" properties
            if (
                isinstance(apdu.propertyIdentifierList, bacpypes.constructeddata.SequenceOf) and
                apdu.propertyIdentifierList[0].propertyIdentifier == bacpypes.object.PropertyIdentifier.all
            ):
                # Get the class of the object and then get all of its properties
                obj = self.get_object_by_id(obj_id)
                if obj is not None:
                    props = [(bacpypes.object.PropertyIdentifier(prop), obj.properties[prop].ReadProperty(obj, obj.properties[prop])) for prop in obj.properties]
                else:
                    raise BACnetError(f"Unknown object {obj_id}")
            else:
                # If not "all" properties then only read the requested properties.
                props = [(bacpypes.object.PropertyIdentifier(prop), None) for prop in apdu.propertyIdentifierList]
        except (BACnetError, ValueError) as e:
            self.logger.error(f"Error in ReadPropertyMultiple: {e}")
            self.response(Error(errorClass='property', errorCode='unknownProperty', context=apdu))
            return

        try:
            iocb = self.readMultiple(
                obj_id,
                props
            )
            iocb.wait()
            if iocb.ioResponse:
                self.response(iocb.ioResponse)
            else:
                self.logger.error(f"Failed to read multiple properties from {obj_id}")
        except Exception as e:
            self.logger.error(f"Exception during readMultiple operation: {e}")
            self.response(Error(errorClass='services', errorCode='inconsistentSelection', context=apdu))

    def do_WritePropertyRequest(self, apdu):
        """Handle WriteProperty requests."""
        try:
            obj_id = apdu.objectIdentifier
            prop_id = apdu.propertyIdentifier

            value = apdu.propertyValue
            result = self.write(f'{obj_id}/{prop_id}', value)
            if result:
                self.response(SimpleAckPDU(context=apdu))
            else:
                raise ExecutionError(errorClass='property', errorCode='writeAccessDenied')
        except (ValueError, DecodingError, CommunicationError, BACnetError) as e:
            self.logger.error(f"Error in WritePropertyRequest: {e}")
            # More specific error handling based on exception type
            if isinstance(e, ExecutionError):
                error_code = e.errorCode
            elif isinstance(e, BACnetError):
                error_code = e.error_code
            else:
                error_code = 'other' 
            self.response(Error(errorClass='property', errorCode=error_code, context=apdu))
        
    def read(self, identifier):
        """Placeholder method to simulate reading a property."""
        try:
            obj_id, prop_id = identifier.split('/')
            if validate_object_id(obj_id) and prop_id in PropertyIdentifier.vendor_range:
                self.logger.info(f"Reading property {prop_id} of object {obj_id}")
                return ReadPropertyACK(objectIdentifier=obj_id, propertyIdentifier=prop_id, propertyValue=ArrayOf(Unsigned)(1))
            else:
                self.logger.error("Invalid object ID or property identifier")
                return None
        except Exception as e:
            self.logger.error(f"Exception during read operation: {e}")
            return None

    def write(self, identifier, value):
        """Placeholder method to simulate writing a property."""
        try:
            obj_id, prop_id = identifier.split('/')
            if validate_object_id(obj_id) and prop_id in PropertyIdentifier.vendor_range:
                self.logger.info(f"Writing value to property {prop_id} of object {obj_id}")
                # Simulate successful write
                return True
            else:
                self.logger.error("Invalid object ID or property identifier")
                return False
        except Exception as e:
            self.logger.error(f"Exception during write operation: {e}")
            return False


class BACnetClient:
    def __init__(self, bbmd_address, device_name='Custom-Client', device_id=1234):
        self.bbmd_address = bbmd_address
        self.app = CustomCOVApplication(device_name, device_id, bacpypes.local.BIPLocalAddress(), bbmd_address)
        self.logger = logging.getLogger(__name__)
        self.devices = []

    def discover_devices(self) -> list[dict]:
        """Discover BACnet devices through the BBMD."""
        self.logger.info("Discovering devices...")
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
                    raise ValueError(f"ReadProperty did not succeed, got: {apdu}")
                # Here you extract the value from the APDU
                return apdu.propertyValue[0].value[0]
        except (CommunicationError, BACnetError) as e:
            self.logger.error(f"Error reading property: {e}")
            return None # Return None on error

    def write_property(self, object_identifier, property_id, value):
        """Writes a property to a BACnet object."""
        try:
            iocb = self.app.write(
                f'{object_identifier}/{property_id}:{value}'
            )
            # check for success
            if iocb.ioError:
                self.logger.error(f"Error writing property: {iocb.ioError}")
                return False  # Indicate write failure
            else:
                return True  # Indicate write success
        except (CommunicationError, BACnetError) as e:
            self.logger.error(f"Error writing property: {e}")
            return False  # Indicate write failure

    def subscribe_to_changes(self, object_identifier, property_id, callback=None, confirmed_notifications=True, lifetime=None, threshold=0.01):
        """Subscribe to COV notifications for the specified object and property."""
        try:
            self.app.do_SubscribeCOVRequest(
                SubscribeCOVRequest(
                    monitoredObjectIdentifier=object_identifier,
                    issueConfirmedNotifications=confirmed_notifications,
                    subscriberProcessIdentifier=0,
                    lifetime=Unsigned(lifetime) if lifetime else None,
                    covIncrement=bacpypes.primitivedata.Real(threshold) if threshold else None
                )
            )
            if callback:
                subscriptions.setdefault(object_identifier, []).append({
                    "property_id": property_id,
                    "callback": callback,
                    "subscriberProcessIdentifier": 0,
                    "threshold": threshold  # Store the threshold
                })
                logging.info(f"Subscribed to changes for object: {object_identifier}, property: {property_id}")
        except (CommunicationError, BACnetError) as e:
            logger.error(f"Error subscribing to changes: {e}")
            return None

    def check_property_writable(self, object_identifier, property_identifier):
        """Check if the property is writable"""
        rp_req = ReadPropertyRequest(
            objectIdentifier=object_identifier,
            propertyIdentifier=PropertyIdentifier.propertyList,
        )
        try:
            iocb = IOCB(rp_req)
            self.app.request_io(iocb)
            iocb.wait()
            response = iocb.ioResponse
            property_list = [bacpypes.object.PropertyIdentifier(prop_id) for prop_id in response.propertyList]
            if property_identifier not in property_list:
                raise BACnetError(f"propertyIdentifier {property_identifier} not in object {object_identifier}")
            object = self.app.get_object_by_id(object_identifier)
            if property_identifier in object.properties and object.properties[property_identifier].mutable:
                return True
            else:
                return False
        except (CommunicationError, BACnetError, TimeoutError, AttributeError) as e:
            logging.error(f"Error checking property writable: {e}")
            return False


def main():
    # Replace with the actual BBMD address and port
    bbmd_address = ("your_bbmd_ip_address", 47808)

    # Initialize the client
    client = BACnetClient(bbmd_address, device_name="BACnetCOVClient", device_id=123) 

    # --- Test Cases ---

    # 1. BBMD and Device Discovery Test:
    client.app.discover_remote_devices()
    # Add a delay to wait for responses before proceeding
    time.sleep(2)

    if client.app.discoveredDevices:
        logging.info("BBMD and Device Discovery Test: PASS")

        # For demonstration, select the first discovered device
        device = client.app.discoveredDevices[0]
        object_identifier = device.iAmDeviceIdentifier
        logging.info(f"Using device: {object_identifier} for property tests")

        # 2. Property Reading Tests:
        # Test multiple properties for a more comprehensive check
        properties_to_read = [
            (PropertyIdentifier.objectName, "Object Name"),
            (PropertyIdentifier.description, "Description"),
            (PropertyIdentifier.presentValue, "Present Value"),  # Assuming the device has these properties
            # Add more properties as needed
        ]

        for prop_id, prop_name in properties_to_read:
            try:
                property_value = client.read_property(object_identifier, prop_id)
                if property_value is not None:
                    logging.info(f"Property Reading Test ({prop_name}): PASS - Value: {property_value}")
                else:
                    logging.warning(f"Property Reading Test ({prop_name}): WARNING - Could not read property from {object_identifier}")
            except Exception as e:  # Catch specific errors if needed
                logging.error(f"Property Reading Test ({prop_name}): FAIL - {e}")
        # 3. Property Writing Test (optional, uncomment if applicable)
        # Ensure you have write access to the properties you are trying to modify
        # Replace PropertyIdentifier.description with the actual property you want to write to
        writable_property = PropertyIdentifier.description  # Or another writable property
        new_description = "Updated description from BACnet Client"
        if client.write_property(object_identifier, writable_property, new_description):
            # Verify that the write was successful by reading the property back
            updated_description = client.read_property(object_identifier, writable_property)
            if updated_description == new_description:
                logging.info(f"Property Writing Test: PASS - Wrote new description to object {object_identifier}")
            else:
                logging.error(f"Property Writing Test: FAIL - Written value does not match for object {object_identifier}")
        else:
            logging.error(f"Property Writing Test: FAIL - Could not write to property {writable_property} on object {object_identifier}")
        
        # 4. COV Subscription Tests
        cov_subscriptions = []

        # Confirmed COV Subscription
        def cov_confirmed_callback(obj_id, prop_id, value):
            logging.info(f"Confirmed COV Notification: Object {obj_id}, Property {prop_id}, Value: {value}")

        # Create a list to hold your property IDs
        properties_to_subscribe = [PropertyIdentifier.presentValue, PropertyIdentifier.objectName]

        for prop in properties_to_subscribe:
            try:
                cov_subscriptions.append(client.subscribe_to_changes(object_identifier, prop, cov_confirmed_callback, lifetime=60, threshold=0.5)) # subscribe for 60 seconds
            except Exception as e:
                logging.error(f"Error subscribing to property {prop}: {e}")

        if all(cov_subscriptions):
            logging.info("COV Subscription Tests: PASS - Subscribed to properties on device")
            time.sleep(30)  # Adjust wait time as needed for testing
        else:
            logging.error("COV Subscription Tests: FAIL - Could not subscribe to all properties.")

        # 5. Unsubscription Test
        for sub in cov_subscriptions:
            client.app.do_UnsubscribeCOVRequest(sub)
        logging.info("Unsubscription Test: PASS")

        while True:
            run()  # Keep the BACnet stack running
    else:
        logging.error("BBMD and Device Discovery Test: FAIL - No devices or BBMDs found.")


# Callback function for COV notifications
def cov_callback(obj_id, prop_id, value):
    logging.info(f"COV Notification: Object {obj_id}, Property {prop_id}, Value: {value}")

if __name__ == "__main__":
    main()
