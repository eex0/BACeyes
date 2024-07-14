import logging
import bacpypes3
from bacpypes3.apdu import ReadPropertyRequest, WritePropertyRequest, ReadPropertyMultipleRequest, WritePropertyMultipleRequest, PropertyValue
from bacpypes3.primitivedata import ObjectIdentifier
from typing import List, Dict, Tuple

from BACeyesApp import BACeyesApp

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)

class BACeyesPropertyAccess:
    def __init__(self, app):
        self.app = app

    async def _get_device_address(self, device_id: int):
        """Helper to fetch the device address or log an error."""
        device_address = self.app.device_info_cache.get_device_address(device_id)
        if not device_address:
            _log.error(f"Device {device_id} not found")
        return device_address

    async def _send_request(self, request):
        """Helper to send a BACnet request and return the response."""
        try:
            return await self.app.request(request)
        except bacpypes3.Exception as e:
            _log.error(f"Error sending BACnet request: {e}")
            return None

    async def read(self, device_id: int, object_type: str, object_instance: int, property_identifier: str):
        """Reads a single property from a BACnet object."""
        device_address = await self._get_device_address(device_id)
        if not device_address:
            return None

        # Create ObjectIdentifier based on type and instance
        object_id = ObjectIdentifier(object_type, object_instance)

        request = ReadPropertyRequest(
            objectIdentifier=object_id,
            propertyIdentifier=property_identifier,
            destination=device_address
        )
        response = await self._send_request(request)
        if response is None:
            return None
        _log.info(f"Read {object_id}.{property_identifier}: {response.propertyValue[0].value}")
        return response.propertyValue[0].value

    async def read_multiple(self, device_id: int, object_type: str, object_instance: int, property_identifiers: List[str]):
        """Reads multiple properties from a BACnet object."""
        device_address = await self._get_device_address(device_id)
        if not device_address:
            return None

        # Create ObjectIdentifier based on type and instance
        object_id = ObjectIdentifier(object_type, object_instance)

        request = ReadPropertyMultipleRequest(
            objectIdentifier=object_id,
            listOfPropertyIdentifiers=property_identifiers,
            destination=device_address
        )
        response = await self._send_request(request)
        if response is None:
            return None
        _log.info(f"Read properties from {object_id} on device {device_id}: {response.listOfReadAccessResults}")
        return response.listOfReadAccessResults

    async def write(self, device_id: int, object_type: str, object_instance: int, property_identifier: str, value, priority=None):
        """Writes a single value to a BACnet property."""
        device_address = await self._get_device_address(device_id)
        if not device_address:
            return
        # Create ObjectIdentifier based on type and instance
        object_id = ObjectIdentifier(object_type, object_instance)

        request = WritePropertyRequest(
            objectIdentifier=object_id,
            propertyIdentifier=property_identifier,
            propertyValue=[PropertyValue(value)],
            priority=priority,
            destination=device_address
        )
        response = await self._send_request(request)
        if response is None:
            _log.error(f"Error writing property {object_id}.{property_identifier} on device {device_id}")
        else:
            _log.info(f"Wrote {object_id}.{property_identifier}: {value}")

    async def write_multiple(self, device_id: int, object_type: str, object_instance: int, property_value_dict: Dict[str, any], priority=None):
        """Writes multiple properties to a BACnet object."""
        device_address = await self._get_device_address(device_id)
        if not device_address:
            return
        # Create ObjectIdentifier based on type and instance
        object_id = ObjectIdentifier(object_type, object_instance)

        write_args = []
        for prop_id, value in property_value_dict.items():
            write_args.append((prop_id, PropertyValue(value), priority))

        request = WritePropertyMultipleRequest(
            objectIdentifier=object_id,
            listOfProperties=write_args,
            destination=device_address
        )
        response = await self._send_request(request)
        if response is None:
            _log.error(f"Error writing properties to {object_id} on device {device_id}")
        else:
            _log.info(f"Wrote properties to {object_id} on device {device_id}: {response.listOfWriteAccessResults}")
            return response.listOfWriteAccessResults