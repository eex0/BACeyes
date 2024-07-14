import asyncio
import logging
from typing import Dict, Optional, Union, List

import bacpypes3
from bacpypes3.comm import Client, bind
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.app import Application
from bacpypes3.pdu import Address, LocalBroadcast
from bacpypes3.apdu import (
    WhoIsRequest,
    IAmRequest,
    ReadPropertyRequest,
    ReadPropertyACK,
)

from BACeyesApp import BACeyesApp

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)

# A dictionary to map device IDs to their addresses
device_address_map: Dict[ObjectIdentifier, Address] = {}

class BACeyesDevices:
    """Represents a BACnet device."""

    def __init__(self, device_id: ObjectIdentifier, device_address: Address, **device_info):
        """Initializes a BACeyesDevice object."""
        self.device_id = device_id
        self.device_address = device_address
        self.device_info = device_info
        self.is_online = False  # Initially assume offline

    async def check_connectivity(self, app: Application) -> None:
        """Checks if the device is online and updates its status."""
        request = ReadPropertyRequest(
            objectIdentifier=self.device_id, propertyIdentifier="objectName"
        )
        try:
            response = await app.request(request, dest=self.device_address)
            if isinstance(response, ReadPropertyACK):
                self.is_online = True
                _log.info(f"Device {self.device_id} is online.")
            else:
                self.is_online = False
                _log.warning(f"Device {self.device_id} is offline.")
        except asyncio.TimeoutError as e:
            self.is_online = False
            _log.warning(
                f"Timeout while communicating with device {self.device_id}: {e}"
            )
        except BaseException as e:
            self.is_online = False
            _log.warning(
                f"Communication error with device {self.device_id}: {e}"
            )

class BACeyesDeviceManager:
    """Manages BACnet device discovery, connections, and information."""

    def __init__(self, local_address: Union[str, Address]):
        """Initializes the BACeyesDeviceManager."""
        self.local_address = local_address
        self.devices: Dict[ObjectIdentifier, BACeyesDevices] = {}
        self.bacnet_app = Application(self, self.local_address)
        self.device_address_map: Dict[ObjectIdentifier, Address] = {}  # Added this line to store discovered device addresses

    async def discover_devices(self, timeout: int = 5) -> None:
        """Discovers BACnet devices on the network."""
        _log.info("Discovering BACnet devices...")
        request = WhoIsRequest()
        request.pduDestination = LocalBroadcast()
        self.bacnet_app.request(request)

        # Give some time for I-Am responses
        await asyncio.sleep(timeout)

        # Initialize the devices dictionary with discovered devices
        for device_id, address in self.device_address_map.items():  # Iterate over the map
            device = BACeyesDevices(device_id, address)
            self.devices[device_id] = device

    def add_device(
        self, device_id: ObjectIdentifier, device_address: Address, **device_info
    ) -> None:
        """Adds a BACnet device to the manager."""
        # ... (unchanged)

    def get_device(self, device_id: ObjectIdentifier) -> Optional["BACeyesDevice"]:
        """Retrieves a BACnet device from the manager."""
        return self.devices.get(device_id)

    async def filter_devices_by_object_types(
        self, object_types: List[str]
    ) -> Dict[ObjectIdentifier, BACeyesDevices]:

        filtered_devices = {}
        for device in self.devices.values():
            request = ReadPropertyRequest(
                objectIdentifier=device.device_id,
                propertyIdentifier='objectList',
            )
            response = await self.bacnet_app.request(request, dest=device.device_address)

            if isinstance(response, ReadPropertyACK):
                object_list = response.propertyValue[0].value  # Assuming a single Application decoder
                for obj in object_list:
                    if obj.objectType.value in object_types:
                        filtered_devices[device.device_id] = device
                        break  # Stop checking this device if we found a matching object
        return filtered_devices


    def on_i_am(self, address: Address, pdu: PDU):
        """Handles I-Am responses during device discovery."""
        if isinstance(pdu, IAmRequest):
            device_id = pdu.iAmDeviceIdentifier
            self.device_address_map[device_id] = address  # Populate the device address map
            _log.info(f"Received I-Am from {device_id} at {address}")

    async def start_device_monitoring(self):
        """Starts monitoring the connections of the added devices."""
        while True:
            for device in self.devices.values():
                if not device.is_online:
                    try:
                        await device.check_connectivity(self.baceyes_app)
                    except asyncio.TimeoutError as e:
                        _log.warning(
                            f"Error checking connectivity for device {device.device_id}: {e}"
                        )
                    except BaseException as e:  # Catch BaseException
                        _log.warning(
                            f"Communication error with device {device.device_id}: {e}"
                        )
            await asyncio.sleep(60)  # Check every 60 seconds

