# BACee - uses:bacpypes3

# Modular BACnet Communication for BBMD Devices: uses bacpypes
# this code provides a framework for BACnet communication and COV subscription management. 
# It allows you to discover BBMDs, monitor BACnet objects for changes, and receive 
# notifications when property values change...

# MIT License - 2024 by: eex0

import asyncio
import logging
import time
import random
import re

from bacpypes3.app import BIPSimpleApplication
from bacpypes3.constructeddata import ArrayOf
from bacpypes3.core import run, stop
from bacpypes3.iocb import IOCB
from bacpypes3.pdu import Address
from bacpypes3.local.device import LocalDeviceObject
from bacpypes3.object import get_object_class, get_datatype, Property, PropertyIdentifier
from bacpypes3.apdu import (
    Error,
    IAmRequest,
    ReadPropertyACK,
    ReadPropertyRequest,
    SimpleAckPDU,
    SubscribeCOVRequest,
    WhoIsRequest,
    WritePropertyRequest,
    ConfirmedCOVNotificationRequest,
    PropertyValue,
)
from bacpypes3.primitivedata import Real, Unsigned
from bacpypes3.errors import DecodingError, ExecutionError, CommunicationError, TimeoutError, BACpypesError

# Logging configuration (detailed format and level)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# Local Device definition
DEVICE_ID = 123
DEVICE_NAME = 'MyDevice'
LOCAL_ADDRESS = '192.168.1.100/24'  # Replace with your device's IP
BBMD_ADDRESS = '192.168.1.255'  # Replace with your BBMD's IP

class COVClient(BIPSimpleApplication):
    def __init__(self, local_address, bbmd_address, device_id, device_name, **kwargs):
        device = LocalDeviceObject(
            objectName=device_name,
            objectIdentifier=('device', device_id),
            maxApduLengthAccepted=1024,
            segmentationSupported='segmentedBoth',
            vendorIdentifier=15,
        )
        super().__init__(device, Address(local_address))

        self.discovered_devices = []
        self.subscriptions = {}
        self.subscription_lock = asyncio.Lock()
        self.obj_value = {}
        self.renewal_tasks = {}

        if bbmd_address:
            self.who_is(destination=Address(bbmd_address))

    async def do_IAmRequest(self, apdu: IAmRequest) -> None:
        _logger.debug(f"Received I-Am Request from {apdu.pduSource}")
        device_info = {
            'device_id': apdu.iAmDeviceIdentifier[1],  # Get device instance number
            'device_address': apdu.pduSource,
            'device_name': apdu.objectName.decode() if apdu.objectName else "Unknown",
            'max_apdu_length_accepted': apdu.maxApduLengthAccepted,
            'segmentation_supported': apdu.segmentationSupported,
            'vendor_id': apdu.vendorID,
        }
        self.discovered_devices.append(device_info)


    async def do_WhoIsRequest(self, apdu: WhoIsRequest) -> None:
        _logger.debug("Received Who-Is Request")

        if self.localDevice is None or self.localDevice.objectName is None or self.localAddress is None:
            return

        low_limit = apdu.deviceInstanceRangeLowLimit
        high_limit = apdu.deviceInstanceRangeHighLimit
        device_id = self.localDevice.objectIdentifier[1]
        if not (low_limit and high_limit) or low_limit <= device_id <= high_limit:
            self.response(
                IAmRequest(
                    iAmDeviceIdentifier=self.localDevice.objectIdentifier,
                    maxApduLengthAccepted=self.localDevice.maxApduLengthAccepted,
                    segmentationSupported=self.localDevice.segmentationSupported,
                    vendorID=self.localDevice.vendorIdentifier,
                )
            )
            
    async def discover_devices(self):
        """Discover all devices on the network with a timeout."""
        _logger.debug("Broadcasting Who-Is request")
        try:
            await asyncio.wait_for(self.who_is(), timeout=5)
        except asyncio.TimeoutError:
            _logger.warning("Who-Is request timed out.")

    async def subscribe_cov(self, device_id, obj_id, property_identifier, confirmed_notifications=True, lifetime_seconds=None):
        """Subscribe to COV notifications, ensuring proper type handling."""
        lifetime = Unsigned(lifetime_seconds) if lifetime_seconds else None

        request = SubscribeCOVRequest(
            subscriberProcessIdentifier=0,
            monitoredObjectIdentifier=obj_id,
            issueConfirmedNotifications=confirmed_notifications,
            lifetime=lifetime,
            monitoredPropertyIdentifier=property_identifier,
        )

        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))

        try:
            await self.request_io(iocb)
            response = iocb.ioResponse
            if not isinstance(response, SimpleAckPDU):
                raise ValueError("Failed to subscribe to COV, response was not SimpleAckPDU")
            _logger.info(f"Subscribed to COV notifications for {obj_id}.{property_identifier}")

            async with self.subscription_lock:
                self.subscriptions.setdefault(obj_id, {})[property_identifier] = True
                self.start_subscription_renewal_task(obj_id, property_identifier, lifetime_seconds, device_id)
        except Exception as e:
            _logger.error(f"Failed to subscribe to COV notifications: {e}")

    def start_subscription_renewal_task(self, obj_id, property_identifier, lifetime_seconds, device_id):
        """Start a task to renew the COV subscription with jitter."""
        async def renew_subscription():
            while self.subscriptions[obj_id][property_identifier]:
                delay = lifetime_seconds - 10
                jitter = random.randint(1, 5)
                delay += jitter
                try:
                    await asyncio.sleep(delay)
                    _logger.info(f"Renewing COV subscription for {obj_id}.{property_identifier}")
                    for retry in range(3):
                        try:
                            await self.subscribe_cov(
                                device_id, obj_id, property_identifier, lifetime_seconds=lifetime_seconds
                            )
                            _logger.info(f"Renewed COV subscription for {obj_id}.{property_identifier}")
                            break
                        except (CommunicationError, TimeoutError) as e:
                            _logger.warning(f"Failed to renew subscription (attempt {retry + 1}): {e}")
                            await asyncio.sleep(2**retry)
                    else:
                        _logger.error(f"Giving up on renewing subscription for {obj_id}.{property_identifier} after 3 attempts")
                        async with self.subscription_lock:
                            self.subscriptions[obj_id][property_identifier] = False
                except asyncio.CancelledError:
                    _logger.info(f"Subscription renewal task for {obj_id}.{property_identifier} cancelled")
                    break

        self.renewal_tasks[(obj_id, property_identifier)] = asyncio.create_task(renew_subscription())

    async def unsubscribe_cov(self, device_id, obj_id, property_identifier):
        """Unsubscribe from COV notifications for a property."""
        request = UnsubscribeCOVRequest(
            subscriberProcessIdentifier=0,
            monitoredObjectIdentifier=obj_id,
            monitoredPropertyIdentifier=property_identifier,
        )
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))

        try:
            await self.request_io(iocb)
            response = iocb.ioResponse
            if isinstance(response, SimpleAckPDU):
                _logger.info(f"Unsubscribed from COV notifications for {obj_id}.{property_identifier}")
                async with self.subscription_lock:  
                    del self.subscriptions[obj_id][property_identifier]
                    self.renewal_tasks[(obj_id, property_identifier)].cancel()
                    del self.renewal_tasks[(obj_id, property_identifier)]
            else:
                _logger.error(f"Failed to unsubscribe from COV notifications for {obj_id}.{property_identifier}")
        except Exception as e:
            _logger.error(f"Error unsubscribing from COV notifications: {e}")


    async def iter_objects(self, device_id):
        """Asynchronously iterate over all objects in a device."""
        request = ReadPropertyRequest(
            objectIdentifier=('device', device_id[1]),
            propertyIdentifier='objectList',
        )
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))

        await self.request_io(iocb)

        if iocb.ioResponse:
            object_list = iocb.ioResponse.propertyValue[0]
            for obj_id in object_list:
                yield obj_id
        else:
            _logger.error(f"Failed to read objectList for device {device_id}")

    async def read_property(self, device_id, obj_id, prop_id):
        """Read a single property with retry logic."""
        request = ReadPropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier=prop_id
        )
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))

        for attempt in range(3):
            try:
                await self.request_io(iocb)
                if iocb.ioResponse:
                    return iocb.ioResponse.propertyValue[0].value[0]
                else:
                    raise BACpypesError("No response received")
            except (CommunicationError, TimeoutError, BACpypesError) as e:
                _logger.warning(f"Error reading property {prop_id} from {obj_id}: {e}, retrying...")
                await asyncio.sleep(1)
        else:
            _logger.error(f"Failed to read property {prop_id} from {obj_id} after multiple retries.")
            return None
            
    async def read_properties(self, device_id, obj_id, property_identifiers):
        """Read multiple properties from a device with error handling and retries."""
        return_dict = {}
        for prop_id in property_identifiers:
            result = await self.read_property(device_id, obj_id, prop_id)
            return_dict[prop_id] = result
        return return_dict

    async def check_writable_properties(self, device_id, object_type, object_instance):
        """Check writable properties with error handling."""
        obj_id = (object_type, object_instance)

        try:
            # Read the 'propertyList' property to get all properties of the object.
            property_list = await self.read_property(device_id, obj_id, 'propertyList')
            if property_list is None:
                _logger.error(f"Failed to read propertyList for object {obj_id}")
                return []

            # Make sure we're only iterating over PropertyIdentifier types
            property_identifiers = [prop.identifier for prop in property_list if isinstance(prop, Property)]

            writable_properties = []
            for prop_id in property_identifiers:
                result = await self.check_property_writable(device_id, obj_id, prop_id)
                if result is not None and result[0].writable:  # Check if result is valid and property is writable
                    writable_properties.append(prop_id)
            return writable_properties
        except Exception as e:  # Catch any unexpected errors
            _logger.error(f"Error checking writable properties for {obj_id}: {e}")
            return []  # Return an empty list on failure


    async def check_property_writable(self, device_id, obj_id, property_identifier):
        """Check if a property is writable by reading its property description."""
        try:
            result = await self.read_property(device_id, obj_id, "propertyDescription")
            if result is not None:  # Check if result is valid
                property_description = result[0]  # Assuming the first element is the property description
                if hasattr(property_description, 'writable') and property_description.writable:
                    return True
                else:
                    return False  # Not writable or doesn't have writable attribute
            else:
                return False  # Result is None, indicating read failure
        except (CommunicationError, TimeoutError, BACpypesError) as e:
            _logger.error(f"Failed to read property description for {obj_id}.{property_identifier}: {e}")
            return None

    async def write_property(self, device_id, obj_id, prop_id, value):
        """Write a value to a BACnet property."""
        try:
            prop_data_type = self.get_data_type(obj_id, prop_id)
            if prop_data_type is None:
                _logger.error(f"Data type for {obj_id}.{prop_id} could not be determined. Skipping write.")
                return

            # Convert value to the property's data type if necessary
            if not isinstance(value, prop_data_type):
                try:
                    value = prop_data_type(value)
                except ValueError as ve:
                    _logger.error(f"Value {value} is not compatible with data type {prop_data_type}: {ve}")
                    return

            request = WritePropertyRequest(
                objectIdentifier=obj_id,
                propertyIdentifier=prop_id,
                propertyArrayIndex=None,
                value=value,
            )
            iocb = IOCB(request)
            iocb.set_destination(Address(device_id))
            await self.request_io(iocb)
            response = iocb.ioResponse
            if isinstance(response, SimpleAckPDU):
                _logger.info(f"Successfully wrote {value} to {obj_id}.{prop_id}")
            else:
                _logger.error(f"Failed to write {value} to {obj_id}.{prop_id}")
        except Exception as e:
            _logger.error(f"Error writing {value} to {obj_id}.{prop_id}: {e}")

    async def monitor_cov(self, obj_id, prop_id, value):
        """Monitor changes in COV subscriptions."""
        try:
            # Check if the property has changed
            if self.obj_value.get(obj_id, {}).get(prop_id) != value:
                self.obj_value.setdefault(obj_id, {})[prop_id] = value
                _logger.info(f"COV update: {obj_id}.{prop_id} changed to {value}")
        except Exception as e:
            _logger.error(f"Error monitoring COV for {obj_id}.{prop_id}: {e}")

    async def handle_cov_notification(self, apdu: ConfirmedCOVNotificationRequest):
        """Handle incoming COV notifications."""
        _logger.info(f"COV Notification received from {apdu.initiatingDeviceIdentifier}: {apdu.monitoredObjectIdentifier}")
        self.obj_value[apdu.monitoredObjectIdentifier] = {
            'process_id': apdu.subscriberProcessIdentifier,
            'time_remaining': apdu.timeRemaining,
            'values': [(prop.propertyIdentifier, prop.value) for prop in apdu.listOfValues]
        }

    async def on_cov_notification(self, apdu: ConfirmedCOVNotificationRequest) -> None:
        """Handle received COV notifications."""
        try:
            _logger.info(f"COV notification received from {apdu.pduSource}")

            for property_value in apdu.listOfValues:
                obj_id = apdu.monitoredObjectIdentifier
                prop_id = property_value.propertyIdentifier
                # Get the correct datatype for casting
                datatype = get_datatype(obj_id[0], prop_id)
                if datatype is None:
                    raise TypeError(f"Unknown datatype for property {prop_id} in object {obj_id}")

                # Explicit cast to the correct datatype
                value = property_value.value.cast_out(datatype)
                _logger.info(f"Object {obj_id}, Property: {prop_id}, Value: {value}")

                self.obj_value.setdefault(obj_id, {})[prop_id] = value

                # Call the monitor_cov method to handle the change
                await self.monitor_cov(obj_id, prop_id, value)
        except Exception as e:
            _logger.error(f"Error processing COV notification: {e}")

    async def handle_apdu(self, apdu):
        """Override handle_apdu to handle different APDU types."""
        _logger.debug(f"APDU received: {apdu}")
        if isinstance(apdu, ConfirmedCOVNotificationRequest):
            await self.on_cov_notification(apdu)

    async def do_ConfirmedCOVNotificationRequest(
        self, apdu: ConfirmedCOVNotificationRequest
    ) -> None:
        """Handle COV notifications with robust error handling and optional ACK delay."""
        try:
            await self.on_cov_notification(apdu)
        except Exception as e:  # Catch-all exception handler
            _logger.error(f"Unexpected error processing COV notification: {e}")
        finally:
            # Always send an ACK, even if there was an error
            self.response(SimpleAckPDU(context=apdu))

    async def async_input(prompt):
        """Asynchronously get input from the user."""
        await asyncio.get_event_loop().run_in_executor(None, input, prompt)

    async def get_data_type(self, obj_id, prop_id):
        """Retrieves the data type of a BACnet property based on the objectType."""
        try:
            # Read the objectType property of the object
            object_type_result = await self.read_property(obj_id, 'objectType')
            if object_type_result is None:
                raise BACpypesError("Failed to read objectType")
            
            object_type = object_type_result[0]
            
            # Get object class based on objectType
            obj_class = get_object_class(object_type)
            if obj_class is None:
                raise ValueError(f"Unknown object type: {object_type}")
            
            # Get the property from the object class
            prop = obj_class.properties.get(prop_id)
            if prop is None:
                raise ValueError(f"Unknown property: {prop_id} for object type: {object_type}")
            
            # Return the data type of the property
            return prop.datatype
        except (ValueError, BACpypesError) as e:
            _logger.error(f"Error retrieving data type for {obj_id}.{prop_id}: {e}")
            return None
 
 
# TEST procedures (will be moved out)
        
 def initialize_bacnet_application():
    # Replace with actual initialization code
    bacnet_app = YourBACnetApplication()  # Instantiate your BACnet application object
    # Initialize any necessary components, such as logging setup, connection establishment, etc.
    bacnet_app.setup()  # Example setup method (replace with your initialization logic)
    return bacnet_app

def cleanup_resources(bacnet_app):
    # Replace with cleanup code
    bacnet_app.teardown()  # Example teardown method (replace with your cleanup logic)
    # Perform any other resource cleanup tasks as needed
           
def test_simple_property_read(bacnet_app):
    # Test 1: Simple Property Read (Easy)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'objectName'    # Property to read
    expected_value = '<expected_value>'  # Expected value of the property

    # Perform property read
    actual_value = bacnet_app.read_property(object_id, property_name)

    # Verify the expected value is logged
    if actual_value == expected_value:
        print(f"Test 1: Simple Property Read - Passed. Value: {actual_value}")
    else:
        print(f"Test 1: Simple Property Read - Failed. Expected: {expected_value}, Actual: {actual_value}")

def test_read_multiple_properties(bacnet_app):
    # Test 2: Read Multiple Properties (Moderate)
    object_id = '<your_object_id>'  # Replace with actual object ID
    properties = ['presentValue', 'objectName', 'units']  # Properties to read
    expected_values = {
        'presentValue': '<expected_present_value>',
        'objectName': '<expected_object_name>',
        'units': '<expected_units>'
    }

    # Perform property read
    actual_values = bacnet_app.read_properties(object_id, properties)

    # Verify all values are returned correctly
    passed = all(actual_values[prop] == expected_values[prop] for prop in properties)
    if passed:
        print("Test 2: Read Multiple Properties - Passed.")
    else:
        print("Test 2: Read Multiple Properties - Failed.")

def test_check_writability(bacnet_app):
    # Test 3: Check Writability (Moderate)
    object_id = '<your_object_id>'  # Replace with actual object ID

    # Check writable properties
    writable_properties = bacnet_app.check_writable_properties(object_id)

    # Verify the check_writable_properties function identifies writable properties correctly
    if writable_properties:
        print(f"Test 3: Check Writability - Passed. Writable Properties: {writable_properties}")
    else:
        print("Test 3: Check Writability - Failed. No writable properties found.")

def test_write_property(bacnet_app):
    # Test 4: Write Property (Moderate)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'presentValue'  # Property to write
    new_value = '<new_value>'  # New value to write

    # Write property
    bacnet_app.write_property(object_id, property_name, new_value)

    # Read back the property to confirm it was written correctly
    actual_value = bacnet_app.read_property(object_id, property_name)

    # Verify the value was successfully written
    if actual_value == new_value:
        print(f"Test 4: Write Property - Passed. New Value: {actual_value}")
    else:
        print(f"Test 4: Write Property - Failed. Expected: {new_value}, Actual: {actual_value}")

def test_cov_subscription_notification(bacnet_app):
    # Test 5: COV Subscription & Notification (Hard)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'presentValue'  # Property to subscribe to

    # Subscribe to COV
    subscription_id = bacnet_app.subscribe_to_cov(object_id, property_name)

    # Simulate device change (manually change value on the device)
    # For simulation, you can adjust a property using a BACnet tool or another script

    # Wait for COV notification (assuming synchronous handling for simplicity)
    time.sleep(5)  # Wait for notification (adjust time as needed)

    # Verify that the client received the COV notification and logged the updated value
    received_value = bacnet_app.get_cov_notification(subscription_id)

    if received_value is not None:
        print(f"Test 5: COV Subscription & Notification - Passed. Received Value: {received_value}")
    else:
        print("Test 5: COV Subscription & Notification - Failed. No COV notification received.")

def test_subscription_renewal(bacnet_app):
    # Test 6: Subscription Renewal (Hard)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'presentValue'  # Property to subscribe to
    subscription_lifetime = 30  # Subscription lifetime in seconds

    # Subscribe to property with short lifetime
    subscription_id = bacnet_app.subscribe_to_cov(object_id, property_name, lifetime=subscription_lifetime)

    # Wait for subscription renewal (assuming synchronous handling for simplicity)
    time.sleep(subscription_lifetime / 2)  # Wait for renewal

    # Verify that the subscription is automatically renewed
    renewed_subscription_id = bacnet_app.get_subscription_id(object_id, property_name)

    if renewed_subscription_id is not None and renewed_subscription_id != subscription_id:
        print("Test 6: Subscription Renewal - Passed. Subscription successfully renewed.")
    else:
        print("Test 6: Subscription Renewal - Failed. Subscription not renewed.")

def test_unsubscription(bacnet_app):
    # Test 7: Unsubscription (Hard)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'presentValue'  # Property to subscribe and then unsubscribe from

    # Subscribe to property
    subscription_id = bacnet_app.subscribe_to_cov(object_id, property_name)

    # Unsubscribe from property
    bacnet_app.unsubscribe_from_cov(subscription_id)

    # Simulate device change (change value after unsubscribing)
    # For simulation, change a property using a BACnet tool or another script

    # Wait for some time to ensure no COV notification is received (assuming synchronous handling for simplicity)
    time.sleep(5)  # Adjust time as needed

    # Verify no COV notification received after unsubscribing
    received_value = bacnet_app.get_cov_notification(subscription_id)

    if received_value is None:
        print("Test 7: Unsubscription - Passed. No COV notification received after unsubscribing.")
    else:
        print("Test 7: Unsubscription - Failed. COV notification received after unsubscribing.")

def test_large_data_transfer(bacnet_app):
    # Test 8: Large Data Transfer (Hard)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = '<property_name_with_large_data>'  # Property that returns large data

    # Read large data property
    large_data = bacnet_app.read_large_data(object_id, property_name)

    # Verify the client handles segmentation correctly and receives complete data
    if large_data is not None:
        print("Test 8: Large Data Transfer - Passed. Large data received successfully.")
    else:
        print("Test 8: Large Data Transfer - Failed. Large data not received or handled correctly.")

def test_error_recovery(bacnet_app):
    # Test 9: Error Recovery (Hard)
    object_id = '<your_object_id>'  # Replace with actual object ID
    property_name = 'presentValue'  # Property to read/write/subscribe for testing error recovery

    # Simulate network errors during operations (disconnect, packet loss)
    # You may need to modify your BACnet client code to simulate and handle these errors

    # Perform BACnet operations and handle errors/retries
    try:
        # Example: Read property with simulated network error
        value = bacnet_app.read_property_with_possible_errors(object_id, property_name)
        print(f"Test 9: Error Recovery - Passed. Value read: {value}")
    except Exception as e:
        print(f"Test 9: Error Recovery - Failed. Error occurred: {str(e)}")

def test_stress_test(bacnet_app):
    # Test 10: Stress Test (Extensive)
    objects_and_properties = [
        ('<object_id_1>', ['<property_1>', '<property_2>', '<property_3>']),
        ('<object_id_2>', ['<property_4>', '<property_5>', '<property_6>'])
        # Add more objects and their properties as needed for stress testing
    ]

    # Subscribe to multiple properties on multiple devices
    subscription_ids = []
    for obj_id, props in objects_and_properties:
        for prop in props:
            subscription_id = bacnet_app.subscribe_to_cov(obj_id, prop)
            subscription_ids.append(subscription_id)

    # Simulate rapid property value changes on devices (manually or via script)

    # Wait for COV notifications (assuming synchronous handling for simplicity)
    time.sleep(10)  # Adjust time as needed based on expected notification rates

    # Verify that the client can handle high volume of COV notifications without issues
    received_notifications = bacnet_app.retrieve_multiple_cov_notifications(subscription_ids)

    if received_notifications:
        print("Test 10: Stress Test - Passed. Received COV notifications successfully.")
    else:
        print("Test 10: Stress Test - Failed. No or incomplete COV notifications received.")

def main():
    # Initialize BACnet application
    bacnet_app = initialize_bacnet_application()

    try:
        # Run tests
        test_simple_property_read(bacnet_app)
        test_read_multiple_properties(bacnet_app)
        test_check_writability(bacnet_app)
        test_write_property(bacnet_app)
        test_cov_subscription_notification(bacnet_app)
        test_subscription_renewal(bacnet_app)
        test_unsubscription(bacnet_app)
        test_large_data_transfer(bacnet_app)
        test_error_recovery(bacnet_app)
        test_stress_test(bacnet_app)
    finally:
        # Clean up resources
        cleanup_resources(bacnet_app)

if __name__ == "__main__":
    main()
                        
