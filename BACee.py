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
from bacpypes3.errors import DecodingError, ExecutionError, CommunicationError, TimeoutError

# Create a logger
logging.basicConfig(level=logging.DEBUG)
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

        # Keep track of discovered devices and COV subscriptions
        self.discovered_devices = []
        self.subscriptions = {}
        self.subscription_lock = asyncio.Lock()
        self.obj_value = {}
        self.renewal_tasks = {}  # To store renewal tasks

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
            subscriberProcessIdentifier=0, 
            monitoredObjectIdentifier=obj_id,
            issueConfirmedNotifications=confirmed_notifications,
            lifetime=Unsigned(lifetime_seconds) if lifetime_seconds is not None else None,
            monitoredPropertyIdentifier=property_identifier,
        )

        # Create an IOCB to track the request
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))  

        try:
            await self.request_io(iocb)
            response = iocb.ioResponse
            if not isinstance(response, SimpleAckPDU):
                raise ValueError("Failed to subscribe to COV, response was not SimpleAckPDU")
            # Subscription successful
            _logger.info(f"Subscribed to COV notifications for {obj_id}.{property_identifier}")

            async with self.subscription_lock:
                if obj_id not in self.subscriptions:
                    self.subscriptions[obj_id] = {}
                if property_identifier not in self.subscriptions[obj_id]:
                    self.subscriptions[obj_id][property_identifier] = True 
                    self.start_subscription_renewal_task(obj_id, property_identifier, lifetime_seconds, device_id)
        except Exception as e:
            _logger.error(f"Failed to subscribe to COV notifications: {e}")


    def start_subscription_renewal_task(self, obj_id, property_identifier, lifetime_seconds, device_id):
        async def renew_subscription():
            while True:
                await asyncio.sleep(lifetime_seconds - 10)  # Start renewal 10 seconds earlier

                _logger.info(f"Renewing COV subscription for {obj_id}.{property_identifier}")
                for retry in range(3):  # Retry up to 3 times with exponential backoff
                    try:
                        await self.subscribe_cov(device_id, obj_id, property_identifier, lifetime_seconds=lifetime_seconds)
                        _logger.info(f"Renewed COV subscription for {obj_id}.{property_identifier}")
                        break 
                    except (CommunicationError, TimeoutError) as e: # catch BACnet specific errors
                        _logger.error(f"Failed to renew subscription (attempt {retry + 1}): {e}")
                        await asyncio.sleep(2 ** retry) # exponential backoff
                else:  # Execute if loop completes without a 'break' (all retries failed)
                    _logger.error(f"Giving up on renewing subscription for {obj_id}.{property_identifier} after {retry + 1} attempts")
                    # Here you would typically remove the subscription or take other corrective action

        asyncio.create_task(renew_subscription())  


    async def iter_objects(self, device_id):
        """Asynchronously iterate over all objects in a device."""
        request = ReadPropertyRequest(
            objectIdentifier=('device', device_id[1]),
            propertyIdentifier='objectList',
        )
        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))

        # Asynchronously send the request
        task = asyncio.create_task(self.request_io(iocb))
        await task  # Wait for the response

        if iocb.ioResponse:
            object_list = iocb.ioResponse.propertyValue[0]
            for obj_id in object_list:
                yield obj_id
        else:
            _logger.error(f"Failed to read objectList for device {device_id}")
            
    async def read_properties(self, device_id, obj_id, property_identifiers):
        """Read multiple properties from a device."""
        # Creating tasks for each property
        tasks = []
        for prop_id in property_identifiers:
            tasks.append(
                asyncio.create_task(
                    self.read_property(device_id, obj_id, prop_id)
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
        await self.request_io(iocb)

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
        iocb.set_destination(Address(device_id))  # Set destination for the request
        await self.request_io(iocb)

        if iocb.ioResponse:
            apdu = iocb.ioResponse
            if not isinstance(apdu, ReadPropertyACK):
                return False

            # Check the access rule from the property description
            prop_description = apdu.propertyValue[0].value[0]
            return prop_description.accessRules & 0x02 != 0  # Check if the property is writable
        else:
            _logger.error(f"Failed to read property description for {obj_id}.{property_identifier}")
            return False

    async def write_property(self, device_id, obj_id, prop_id, value, priority=None):
        """Writes a property to a BACnet object."""
        datatype = get_datatype(obj_id[0], prop_id)
        if not datatype:
            raise ValueError("Invalid property for object type")

        request = WritePropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier=prop_id,
            propertyValue=PropertyValue(
                propertyIdentifier=prop_id,
                value=datatype(value),
                priority=priority,
            ),
        )

        iocb = IOCB(request)
        iocb.set_destination(Address(device_id))  # Set the destination address for the request

        await self.request_io(iocb)  # Use await here

        if iocb.ioResponse and isinstance(iocb.ioResponse, SimpleAckPDU):
            logging.info(f"Successfully wrote property {prop_id} to {obj_id}")
        else:
            logging.error(f"Failed to write property {prop_id} to {obj_id}")

    async def monitor_cov(self, obj_id, prop_id, value):
        """Monitor changes in COV subscriptions."""
        try:
            # Check if the property has changed
            if self.obj_value.get(obj_id, {}).get(prop_id) != value:
                self.obj_value.setdefault(obj_id, {})[prop_id] = value
                _logger.info(f"COV update: {obj_id}.{prop_id} changed to {value}")
        except Exception as e:
            _logger.error(f"Error monitoring COV for {obj_id}.{prop_id}: {e}")

    async def on_cov_notification(self, apdu):
        """Handle received COV notifications."""
        try:
            _logger.info(f"COV notification received from {apdu.pduSource}")

            for property_value in apdu.listOfValues:
                obj_id = apdu.monitoredObjectIdentifier
                prop_id = property_value.propertyIdentifier
                value = property_value.value.tagValue
                _logger.info(f"Object {obj_id}, Property: {prop_id}, Value: {value}")

                self.obj_value.setdefault(obj_id, {})[prop_id] = value
        except Exception as e:
            _logger.error(f"Error processing COV notification: {e}")
    
    async def handle_apdu(self, apdu):
        """Override handle_apdu to handle different APDU types."""
        _logger.debug(f"APDU received: {apdu}")
        if isinstance(apdu, ConfirmedCOVNotificationRequest):
           await self.on_cov_notification(apdu)
            
    async def do_ConfirmedCOVNotificationRequest(self, apdu: ConfirmedCOVNotificationRequest) -> None:
        """Handle COV notifications more robustly."""
        _logger.info(f"Received COV notification: {apdu}")

        try:
            for element in apdu.listOfValues:
                prop_id = element.propertyIdentifier
                value = element.value.cast_out(get_datatype(apdu.monitoredObjectIdentifier[0], prop_id))

                obj_id = apdu.monitoredObjectIdentifier

                self.obj_value.setdefault(obj_id, {})[prop_id] = value
                await self.monitor_cov(obj_id, prop_id, value)  # Use monitor_cov here

                _logger.info(f"COV notification - Object: {obj_id}, Property: {prop_id}, Value: {value}")
        except Exception as e:
            _logger.error(f"Error processing COV notification: {e}")
        finally:
            # Always send an ACK even if there was an error
            self.response(SimpleAckPDU(context=apdu))


async def main():
    """The heart of the BACnet adventure! This function orchestrates the discovery, reading,
    writing, and subscription to COV notifications on BACnet devices.
    """

    try:
        # Create your trusty BACnet sidekick (the client)
        app = COVClient(LOCAL_ADDRESS, BBMD_ADDRESS, DEVICE_ID, DEVICE_NAME)

        # Unleash the Who-Is spell to uncover hidden BACnet devices
        _logger.debug("Embarking on a quest to discover BACnet devices...")
        await asyncio.create_task(app.discover_devices())
        _logger.info(f"Behold, the discovered devices: {app.discovered_devices}")

        if not app.discovered_devices:  # A desolate BACnet landscape?
            _logger.error("No devices found! The quest has failed.")
            return

        # Choose your target like a wise wizard
        target_device = None
        while target_device is None:
            _logger.info("Pick your BACnet target:")
            for i, device in enumerate(app.discovered_devices):
                _logger.info(f"{i+1}. {device['device_id']} ({device['device_name']})")
            choice = input("Enter the number of the chosen one: ")
            try:
                index = int(choice) - 1  # Humans count from 1, computers from 0
                if 0 <= index < len(app.discovered_devices):
                    target_device = app.discovered_devices[index]
                else:
                    _logger.error("That number isn't on the list, oh wise one. Try again.")
            except ValueError:  # Oh no, a non-numeric incantation!
                _logger.error("Numbers, not letters, are the key to this magic!")

        device_id = target_device['device_address']
        device_address = target_device['device_address']

        target_object = None
        while target_object is None:
            _logger.info("Unveil the secrets of an object within the chosen device:")
            async for obj_id in app.iter_objects(device_id):
                _logger.info(f"{obj_id} ({app.get_object_name(obj_id)})")  # Reveal object IDs and their cryptic names
            choice = input("Enter the object identifier (e.g., 'analogInput, 0') to probe its depths: ")
            try:
                object_type, object_instance = choice.split(",")
                obj_identifier = (object_type.strip(), int(object_instance.strip()))  # Parse the incantation
                if app.get_object_by_id(obj_identifier):
                    target_object = obj_identifier  # The object has been located!
                else:
                    _logger.error("That object eludes my senses. Try another identifier.")
            except ValueError:  # A garbled incantation!
                _logger.error("Incorrect incantation format. Utter 'objectType, instanceNumber'.")

        # 1. Read All Properties:
        _logger.info(f"Peering into the depths of object {obj_identifier} on device {device_id}...")
        properties = await app.read_properties(device_id, obj_identifier, [PropertyIdentifier.all])
        _logger.info(f"Revealed properties: {properties}")

        # 2. Property Writability Check:
        _logger.info(f"Testing the malleability of object {obj_identifier} on device {device_id}...")
        writable_properties = await app.check_writable_properties(device_id, *obj_identifier)
        _logger.info(f"Properties open to change: {writable_properties}")

        # 3. Property Write Test:
        if writable_properties:
            chosen_property = None
            while chosen_property is None:
                _logger.info("Choose a property to inscribe with your will:")
                for i, prop in enumerate(writable_properties):
                    _logger.info(f"{i+1}. {prop}")  # Present the options
                property_choice = input("Enter the number of your chosen property: ")
                try:
                    index = int(property_choice) - 1  # Mortals count differently...
                    if 0 <= index < len(writable_properties):
                        chosen_property = writable_properties[index]
                    else:
                        _logger.error("Invalid choice. Speak a valid number!")
                except ValueError:  # The old 'number, not letter' trick
                    _logger.error("Numbers, not symbols, are the language of change!")

            datatype = get_datatype(obj_identifier[0], chosen_property)
            if datatype:
                value_to_write = None
                while value_to_write is None:
                    try:
                        value_str = input(f"Inscribe the new value for {chosen_property} (type: {datatype.__name__}): ")
                        value_to_write = datatype(value_str)  # The transformation ritual
                    except ValueError:
                        _logger.error(f"The inscription must conform to the {datatype.__name__} form.")

                await app.write_property(device_id, obj_identifier, chosen_property, value_to_write)
            else:
                _logger.error(f"The property {chosen_property} resists my magic...")  # Data type not supported

        else:
            _logger.info(f"Alas, the object {obj_identifier} is unyielding...")  # No writable properties

        # 4. COV Subscription Test:
        property_to_subscribe = 'presentValue'  # The property to watch closely
        lifetime_seconds = 300  # Watch for 5 minutes
        _logger.info(f"Now observing changes to {obj_identifier}.{property_to_subscribe}...")
        await app.subscribe_cov(device_id, obj_identifier, property_to_subscribe, lifetime_seconds=lifetime_seconds)

    except KeyboardInterrupt:
        _logger.debug("The ritual has been interrupted.")  # User pressed Ctrl+C
    finally:
        stop()  # Clean up and extinguish the magic flames

if __name__ == "__main__":
    asyncio.run(main())  # Ignite the incantation!
