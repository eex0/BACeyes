# BACee - uses:bacpypes3

# Modular BACnet Communication for BBMD Devices: uses bacpypes
# this code provides a framework for BACnet communication and COV subscription management. 
# It allows you to discover BBMDs, monitor BACnet objects for changes, and receive 
# notifications when property values change...

# MIT License - 2024 by: eex0

import asyncio
import logging
import time

from bacpypes3.app import BIPSimpleApplication
from bacpypes3.constructeddata import ArrayOf
from bacpypes3.core import run, stop
from bacpypes3.iocb import IOCB
from bacpypes3.pdu import Address
from bacpypes3.local.device import LocalDeviceObject
from bacpypes3.object import get_datatype, PropertyIdentifier, AnalogValueObject
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
from bacpypes3.errors import DecodingError, ExecutionError

# Create a logger
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
_logger.addHandler(handler)

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

        # Keep track of discovered devices and COV subscriptions
        self.discovered_devices = []
        self.subscriptions = {}
        self.subscription_lock = asyncio.Lock()
        self.obj_value = {}

        # Configure BBMD if provided
        if bbmd_address:
            self.who_is(destination=Address(bbmd_address))

    async def do_IAmRequest(self, apdu):
        """Process received I-Am requests."""
        _logger.debug("Received I-Am Request")
        device_info = {
            'device_id': apdu.iAmDeviceIdentifier,
            'device_address': apdu.pduSource,
            'device_name': apdu.objectName,
            'max_apdu_length_accepted': apdu.maxApduLengthAccepted,
            'segmentation_supported': apdu.segmentationSupported,
            'vendor_id': apdu.vendorID,
        }
        self.discovered_devices.append(device_info)

    async def do_WhoIsRequest(self, apdu):
        """Respond to Who-Is requests."""
        _logger.debug("Received Who-Is Request")
        # If the device is a BBMD, it will ignore the request
        if self.localAddress is None:
            self.response(IAmRequest())

    async def discover_devices(self):
        """Discover all devices on the network."""
        await self.who_is()  # Broadcast a Who-Is request
        await asyncio.sleep(1)  # Give some time for I-Am responses


    async def subscribe_cov(self, device_id, obj_id, property_identifier, confirmed_notifications=True, lifetime_seconds=None):
        """Subscribe to COV notifications for a property."""
        request = SubscribeCOVRequest(
            subscriberProcessIdentifier=0,  # Assuming only one subscriber for this example
            monitoredObjectIdentifier=obj_id,
            issueConfirmedNotifications=confirmed_notifications,
            lifetime=Unsigned(lifetime_seconds) if lifetime_seconds is not None else None,
            monitoredPropertyIdentifier=property_identifier,
        )
        
        # Create an IOCB to track the request
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))  # Set the destination address for the request

        try:
            await self.request_io(iocb)
            response = iocb.ioResponse
            if not isinstance(response, SimpleAckPDU):
                raise ValueError("Failed to subscribe to COV, response was not SimpleAckPDU")

            _logger.info(f"Subscribed to COV notifications for {obj_id}.{property_identifier}")

            async with self.subscription_lock:
                self.subscriptions.setdefault(obj_id, []).append(property_identifier)
                self.start_subscription_renewal_task(obj_id, property_identifier, lifetime_seconds)
        except Exception as e:
            _logger.error(f"Failed to subscribe to COV notifications: {e}")

def start_subscription_renewal_task(self, obj_id, property_identifier, lifetime_seconds, device_id):
    """Starts a background task to renew the COV subscription periodically."""
    async def renew_subscription():
        """
        Coroutine that handles subscription renewal.
        """
        while True:
            try:
                await asyncio.sleep(lifetime_seconds)  # Wait for the lifetime duration
                _logger.info(f"Renewing COV subscription for {obj_id}.{property_identifier}")
                try:
                    # Pass the device_id to subscribe_cov
                    await self.subscribe_cov(device_id, obj_id, property_identifier, lifetime_seconds=lifetime_seconds)
                    _logger.info(f"Renewed COV subscription for {obj_id}.{property_identifier}")
                except Exception as e:
                    _logger.error(f"Failed to renew subscription: {e}")
                    # Optionally, you can add retry logic here

            except asyncio.CancelledError:
                _logger.info(f"Subscription renewal task for {obj_id}.{property_identifier} cancelled.")
                break  # Exit the loop if the task is cancelled

    # Store the task so you can cancel it later if needed
    self.renewal_tasks[(obj_id, property_identifier)] = asyncio.create_task(renew_subscription())



    async def read_properties(self, device_id, obj_id, property_identifiers):
        """Read multiple properties from a device."""
        # Creating tasks for each property
        tasks = []
        for prop_id in property_identifiers:
            tasks.append(
                asyncio.create_task(
                    self.read_property(device_id, *obj_id, prop_id)
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return_dict = {}
        for prop_id, value in zip(property_identifiers, results):
            if isinstance(value, Exception):
                _logger.error(f"Error reading property {prop_id} from {obj_id}: {value}")
            else:
                return_dict[prop_id] = value

        return return_dict


    async def check_writable_properties(self, device_id, object_type, object_instance):
        """Reads the Property List object to determine which properties are writable for a given object."""

        obj_id = (object_type, object_instance)
        request = ReadPropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier='propertyList',
        )
        iocb = IOCB(request)
        iocb.set_timeout(10)
        iocb.set_destination(Address(device_id))  # Set destination for the request
        task = asyncio.create_task(self.request_io(iocb))
        await task

        if iocb.ioResponse:
            apdu = iocb.ioResponse
            if not isinstance(apdu, ReadPropertyACK):
                _logger.error("Failed to read propertyList, response was not ReadPropertyACK")
                return []

            property_identifiers = [prop_id.value for prop_id in apdu.propertyValue[0]]
            mutable_properties = []

            for prop_id in property_identifiers:
                # Create a task for each property and gather the results
                task = asyncio.create_task(self.check_property_writable(device_id, obj_id, prop_id))
                mutable_properties.append(task)

            writable_props = await asyncio.gather(*mutable_properties)
            return [prop for prop, is_mutable in zip(property_identifiers, writable_props) if is_mutable]
        else:
            _logger.error("Failed to read propertyList")
            return []  

    async def check_property_writable(self, device_id, obj_id, property_identifier):
        """Check if the property is writable by reading its property description."""
        request = ReadPropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier='propertyDescription'
        )

        iocb = IOCB(request)
        iocb.set_timeout(5)  # Set a timeout for the request
        await self.request_io(iocb)  # Use await here

        if iocb.ioResponse:
            apdu = iocb.ioResponse
            if not isinstance(apdu, ReadPropertyACK):
                _logger.error(
                    f"Failed to read property description for {obj_id}.{property_identifier}: {apdu}"
                )
                return False

            property_description = apdu.propertyValue[0]
            for item in property_description:
                if item.propertyIdentifier == "mutable" and item.value[0].value:
                    return True

        return False

            
    async def write_property(self, device_id, obj_id, prop_id, value, priority=None):
        """Write a property value to a device."""

        # Check property writability first
        if not await self.check_property_writable(device_id, obj_id, prop_id):
            _logger.error(f"Property {prop_id} is not writable for object {obj_id}")
            return

        # Construct the WriteProperty request
        value = get_datatype(obj_id[0], prop_id)(value)
        request = WritePropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier=prop_id,
            propertyValue=[value],
        )
        if priority is not None:
            request.priority = Unsigned(priority)
        request.pduDestination = Address(device_id) # set the destination of the request

        # Create an IOCB and send the request
        iocb = IOCB(request)
        try:
            await self.request_io(iocb)
            response = iocb.ioResponse
            if not isinstance(response, SimpleAckPDU):
                raise ValueError("Failed to write property")
            _logger.info(f"Successfully wrote {value} to {obj_id}.{prop_id}")
        except Exception as e:
            _logger.error(f"Failed to write property: {e}")

    def do_ConfirmedCOVNotificationRequest(self, apdu: ConfirmedCOVNotificationRequest) -> None:
        """Handle COV notifications."""
        _logger.info(f"Received COV notification: {apdu}")
        # Get the value from the notification
        for element in apdu.listOfValues:
            prop_id = element.propertyIdentifier
            value = element.value[0]

            # Extract the object identifier
            obj_id = apdu.monitoredObjectIdentifier

            # Update the stored value for this object
            self.obj_value[obj_id] = {prop_id[0]:value}  # Create new value dict

            # Call the callback if it's registered
            # Ensure the callback dictionary exists for this object and property
            callbacks = self._callbacks.get(obj_id, {}).get(prop_id[0], []) 
            for callback in callbacks:
                callback(obj_id, prop_id[0], value)  # Assuming the first element is the property value


        # Send an acknowledgement
        self.response(SimpleAckPDU(context=apdu))


async def read_write_subscribe_loop(app, device):
    """Asynchronous generator to continuously read, write, and subscribe to a device."""

    # Find the first valid object in the device
    for obj in app.iter_objects(device["device_id"]):
        obj_identifier = (obj.objectType, obj.objectInstance)
        if obj is not None:
            break
    else:  # No valid object found
        _logger.error("No valid object found on the device.")
        return

    while True:
        _logger.info(f"Working on object: {obj_identifier}")

        # Read all properties of the object
        props = await app.read_properties(
            device['device_id'],
            obj_identifier,
            [PropertyIdentifier.all]
        )
        _logger.info(f"Read properties of the object {obj_identifier}: {props}")

        # Determine writable properties
        writable_properties = await app.check_writable_properties(
            device['device_id'],
            obj_identifier[0],
            obj_identifier[1]
        )
        _logger.info(f"Writable properties for object {obj_identifier}: {writable_properties}")

        # Write to a specific writable property (if it's available)
        prop_to_write = 'presentValue'  # Replace with the property you want to write
        if prop_to_write in writable_properties:
            datatype = get_datatype(obj_identifier[0], prop_to_write)
            if datatype:
                value_to_write = datatype(100.5)  # Get input from the user or another source
                await app.write_property(device['device_id'], obj_identifier, prop_to_write, value_to_write)
            else:
                _logger.error(f"Invalid data type for property {prop_to_write}")
        else:
            _logger.info(f"Property {prop_to_write} is not writable for object {obj_identifier}")

        # Subscribe to changes in the object's presentValue
        await app.subscribe_cov(
            device['device_id'], obj_identifier, 'presentValue', lifetime_seconds=300
        )
        await asyncio.sleep(1) # pause 1 second before checking next object


async def read_properties(self, device_id, obj_id, property_identifiers):
    """Read multiple properties from a device."""
    results = {}
    for prop_id in property_identifiers:
        try:
            value = await self.read_property(device_id, *obj_id, prop_id) # unpacking obj_id
            if value is not None:
                results[prop_id] = value
        except Exception as e:
            _logger.error(f"Error reading property {prop_id} from {obj_id}: {e}")
    return results

async def main():
    # ... (initializations and device discovery are the same as before) ...
    
    # Choose the first discovered device (you can change this logic)
    device = app.discovered_devices[0]

    # Read all properties of a specific object in the device
    object_type = 'analogInput'    # Replace with desired object type
    object_instance = 0           # Replace with desired object instance
    obj_identifier = (object_type, object_instance)

    # Read the properties of the selected object
    props = await app.read_properties(device['device_id'], obj_identifier, [PropertyIdentifier.all])
    _logger.info(f"Read properties of object {obj_identifier}: {props}")

    # Determine writable properties
    writable_properties = await app.check_writable_properties(device['device_id'], object_type, object_instance)
    _logger.info(f"Writable properties for object {obj_identifier}: {writable_properties}")

    # Write to a specific writable property (if it's available)
    prop_to_write = 'presentValue'  # Replace with the property you want to write
    if prop_to_write in writable_properties:
        datatype = get_datatype(object_type, prop_to_write)
        if datatype:
            value_to_write = datatype(100.5)  # Get input from the user or another source
            await app.write_property(device['device_id'], obj_identifier, prop_to_write, value_to_write)
        else:
            _logger.error(f"Invalid data type for property {prop_to_write}")
    else:
        _logger.info(f"Property {prop_to_write} is not writable for object {obj_identifier}")

    # Subscribe to changes in the first object's presentValue
    await app.subscribe_cov(
        device['device_id'], obj_identifier, 'presentValue', lifetime_seconds=300
    )

    # Run the BACnet application
    _logger.debug("Running")
    while True:  
        await asyncio.sleep(1)  

    except KeyboardInterrupt:
        _logger.debug("keyboard interrupt")
    finally:
        stop()

if __name__ == "__main__":
    asyncio.run(main())
