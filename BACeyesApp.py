import bacpypes3
import asyncio
import logging
import os
import json
from typing import Dict, Callable, Any, Union

import bacpypes3
from bacpypes3.app import Application
from bacpypes3.pdu import Address
from bacpypes3.comm import bind
from bacpypes3.primitivedata import ObjectIdentifier, PropertyIdentifier
from bacpypes3.apdu import ConfirmedCOVNotificationRequest

from BACeyesDevices import BACeyesDeviceManager
from BACeyesCOV import BACeyesCOV
from BACeyesAlarms import BACeyesAlarmManager
from BACeyesPropAccess import BACeyesPropertyAccess


# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)


class DeviceProperty:
    """Represents a BACnet object property on a specific device."""

    def __init__(self, device_id: int, obj_id: ObjectIdentifier, prop_id: str):
        self.device_id = device_id
        self.obj_id = obj_id
        self.prop_id = prop_id

    def __hash__(self):
        """Custom hash function, allows using DeviceProperty as keys in dictionaries."""
        return hash((self.device_id, self.obj_id, self.prop_id))


class BACeyesApp(Application):
    """Main application class for BACeyes."""

    def __init__(
        self, local_address: Union[str, Address], config_file_path: str = "baceyes_config.json"
    ):
        super().__init__(local_address)
        self.device_manager = BACeyesDeviceManager(self)
        self.cov_manager = BACeyesCOV(self)
        self.alarm_manager = BACeyesAlarmManager(self)
        self.property_access = BACeyesPropertyAccess(self)
        self.config_file_path = config_file_path
        self.config = {}
        self.load_config()

    def load_config(self):
        """Loads configuration from the specified JSON file."""
        try:
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, "r") as f:
                    self.config = json.load(f)
            else:
                self.config = {}  # Use empty dictionary if config file doesn't exist
                _log.warning("Configuration file not found. Using default settings.")
        except Exception as e:
            _log.error(f"Error loading configuration: {e}")
            self.config = {}

    def register_for_confirmed_cov_notifications(self):
        """Registers this application to receive ConfirmedCOVNotification requests."""
        self.this_application.add_capability(ConfirmedCOVNotificationRequest)

    async def do_ConfirmedCOVNotificationRequest(self, apdu: ConfirmedCOVNotificationRequest):
        """Handles incoming ConfirmedCOVNotification requests."""
        await self.cov_manager.handle_cov_notification(apdu)

    async def run(self):
        """Starts the main BACeyes application tasks."""
        try:
            bind(self.cov_manager, self)
            self.register_for_confirmed_cov_notifications()

            # Run device discovery, start monitoring, and COV subscriptions
            await asyncio.gather(
                self.device_manager.discover_devices(),
                self.device_manager.start_device_monitoring(),
                self.cov_manager.manage_subscriptions(),
            )
            # Run BACnet stack
            await self.async_run()

        except Exception as e:
            _log.exception(f"Error running BACeyes application: {e}")

    async def async_run(self):
        """Run the BACnet stack."""
        await super().run()


    def subscribe_to_cov(self, device_property: DeviceProperty, callback: Callable):
        """Subscribes to COV notifications for a specific property.

        Args:
            device_property (DeviceProperty): The device property to subscribe to.
            callback (Callable): The function to call when a notification is received.
        """
        return asyncio.create_task(self.cov_manager.subscribe(
            device_property.device_id,
            device_property.obj_id,
            device_property.prop_id,
            callback
        ))

    def unsubscribe_from_cov(self, device_property: DeviceProperty):
        """Unsubscribes from COV notifications for a specific property.

        Args:
            device_property (DeviceProperty): The device property to unsubscribe from.
        """
        return asyncio.create_task(self.cov_manager.unsubscribe(
            device_property.device_id,
            device_property.obj_id,
            device_property.prop_id
        ))

    def run_bacnet_stack(self):
        """Run the BACnet stack in a separate thread."""
        try:
            _log.info("Starting BACnet stack")
            asyncio.run(self.bacnet_app.run())  # Use asyncio.run to run the event loop for BACnet
        except Exception as e:
            _log.exception("Error in BACnet stack:", exc_info=e)

    async def read_property(
            self, device_property: DeviceProperty, timeout: int = 5
    ) -> Any:
        """Reads the value of a specific property on a BACnet device.

        Args:
            device_property: A DeviceProperty object specifying the device, object, and property.
            timeout (int, optional): Request timeout in seconds. Defaults to 5.

        Returns:
            The value of the property, or None if an error occurs.
        """
        return await self.property_access.read(  # Use self.property_access to call the read method
            device_property.device_id,
            device_property.obj_id,
            device_property.prop_id,  # Pass property_identifier
            timeout=timeout,
        )
