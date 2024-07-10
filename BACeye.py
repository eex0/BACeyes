# BACeyepys - uses:bacpypes3

# Modular BACnet Communication for BBMD Devices: uses bacpypes

# MIT License - 2024 by: eex0


# Overall, this code serves as a robust foundation for building BACnet applications that require real-time monitoring 
# and control of devices across potentially complex network topologies.
# It can be customized and expanded to suit the specific requirements of various BACnet-based systems.

# Notice: This is a work in progress, there ARE bugs here!

import asyncio
import json
import logging
import logging.handlers
import os
import time
import re
import sys
import sqlite3
from datetime import datetime
from collections import defaultdict
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import numpy as np
import matplotlib.pyplot as plt
import threading

from bacpypes3.app import BIPSimpleApplication
from bacpypes3.constructeddata import ArrayOf, SequenceOf
from bacpypes3.core import run, stop
from bacpypes3.pdu import Address
from bacpypes3.local.device import LocalDeviceObject
from bacpypes3.object import get_object_class, get_datatype, PropertyIdentifier
from bacpypes3.primitivedata import Unsigned
from bacpypes3.apdu import (
    Error,
    IAmRequest,
    ReadPropertyACK,
    ReadPropertyRequest,
    RegisterForeignDeviceRequest,
    SimpleAckPDU,
    SubscribeCOVRequest,
    UnsubscribeCOVRequest,
    WhoIsRequest,
    WritePropertyRequest,
    ConfirmedCOVNotificationRequest,
    PropertyValue,
    ReadPropertyMultipleRequest,
    WritePropertyMultipleRequest,
    ReadPropertyMultipleACK,
)
from bacpypes3.primitivedata import Real, Unsigned
from bacpypes3.errors import DecodingError, ExecutionError, BACpypesError
from bacpypes3.local.object import AnalogInputObject
from bacpypes3.settings import settings
from bacpypes3.debugging import ModuleLogger, bacpypes_debugging

# ChangeOfValueServices
from bacpypes3.service.cov import ChangeOfValueServices, SubscriptionContextManager

#DeviceInfoCache
from bacpypes3.netservice import NetworkServiceAccessPoint, NetworkServiceElement
from bacpypes3.comm import bind
from bacpypes3.ipv4.service import BIPForeign, UDPMultiplexer

from jsonschema import validate

from flask import Flask, jsonify, request
from flask_httpauth import HTTPBasicAuth
from flask_socketio import SocketIO, emit
from itsdangerous import TimestampSigner
import jwt  # Import PyJWT library for JWT handling
from functools import wraps
from flask import session, request, jsonify
from flask_socketio import disconnect
 

# ******************************************************************************

# Logging Configuration (with Console and File Logging)
_debug = 0
_log = ModuleLogger(globals())  # Initialize the module logger
logging.basicConfig(
    filename="bacee.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            "bacee.log", maxBytes=1024 * 1024 * 5, backupCount=5
        ),
        logging.StreamHandler(sys.stdout),  # Add console handler
    ],
)
logger = logging.getLogger(__name__)

# ******************************************************************************

# --- Device & Network Configuration (from JSON) ---
with open("network_topology.json", "r") as f:
    config = json.load(f)
local_device_config = config.get("local_device", {})
DEVICE_ID = local_device_config.get("device_id", 123)
DEVICE_NAME = local_device_config.get("device_name", "MyDevice")
LOCAL_ADDRESS = local_device_config.get("local_address", "192.168.1.100/24")
BROADCAST_ADDRESS = "192.168.1.255"
BBMD_ADDRESS = local_device_config.get("bbmd_address", "192.168.1.255")


# ******************************************************************************

# TODO - Define custom exceptions for network communication and timeouts
class CommunicationError(Exception):
    pass

class TimeoutError(Exception):
    pass
    
# Subscription: deals with subscriptions to receive updates on specific properties

class Subscription:
    def __init__(self, device_id, obj_id, prop_id, confirmed_notifications=True, lifetime_seconds=None,
        change_filter=None, priority=None,
    ):
        self.device_id = device_id
        self.obj_id = obj_id
        self.prop_id = prop_id
        self.confirmed_notifications = confirmed_notifications
        self.lifetime_seconds = lifetime_seconds
        self.active = True
        self.change_filter = change_filter
        self.priority = priority
        self.alarms = []
        self.context_manager = None  # To store the SubscriptionContextManager


    async def renew_subscription(self, app: BACeeApp, timeout: int = 5):
        """Renews the COV subscription."""
        if not self.active:
            _logger.warning(
                f"Trying to renew an inactive subscription for {self.obj_id}.{self.prop_id}"
            )
            return

        _logger.info(f"Renewing COV subscription for {self.obj_id}.{self.prop_id}")

        try:
            # Use existing context manager
            await app.subscribe_cov(self, renew=True)

            _logger.info(f"Renewed COV subscription for {self.obj_id}.{self.prop_id}")
        except (CommunicationError, TimeoutError, asyncio.TimeoutError) as e:
            _logger.warning(
                f"Error renewing subscription for {self.obj_id}.{self.prop_id}: {e}"
            )
            self.active = False  # Mark subscription as inactive on error


                    
# ******************************************************************************


class PropertyReader:
    def __init__(self, app: BACeeApp):
        self.app = app

    async def read_property(self, device_id, obj_id, prop_id):
        """Reads a single property, caching the result in DeviceInfoCache."""
        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if not device_info:
            _logger.error(f"Device with ID {device_id} not found.")
            return None

        # Check if the property is already cached in DeviceInfoCache
        cached_value = device_info.get_object_property(obj_id, prop_id)
        if cached_value is not None:
            return cached_value

        # Property not cached, read from device
        request = ReadPropertyRequest(
            objectIdentifier=obj_id, propertyIdentifier=prop_id
        )
        request.pduDestination = device_info.address
        try:
            response = await self.app.request(request)
            if isinstance(response, ReadPropertyACK):
                value = response.propertyValue
                # Cache the value in DeviceInfoCache
                device_info.set_object_property(obj_id, prop_id, value)
                return value
            else:
                _logger.error(
                    f"Error reading property {obj_id}.{prop_id} on device {device_id}: {response}"
                )
        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Communication error with device {device_id}: {e}")
        return None

    async def read_multiple_properties(self, device_id, obj_id_prop_id_list):
        """Reads multiple properties from multiple objects on a device using RPM."""

        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if not device_info:
            _logger.error(f"Device with ID {device_id} not found.")
            return None

        # Group properties by object ID to create ReadAccessSpecification
        object_property_map = defaultdict(list)
        for obj_id, prop_id in obj_id_prop_id_list:
            object_property_map[obj_id].append(prop_id)

        read_access_specs = []
        for obj_id, prop_ids in object_property_map.items():
            read_access_specs.append((obj_id, prop_ids))

        request = ReadPropertyMultipleRequest(
            device_address=device_info.address, properties=read_access_specs
        )

        try:
            response = await self.app.request(request)
            if isinstance(response, ReadPropertyMultipleACK):
                values = {}
                for obj_prop_list in response.values:
                    for prop_value in obj_prop_list:
                        values[
                            (
                                prop_value.objectIdentifier,
                                prop_value.propertyIdentifier,
                            )
                        ] = prop_value.value
                return values
            else:
                _logger.error(
                    f"Error reading multiple properties from device {device_id}: {response}"
                )
        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Communication error with device {device_id}: {e}")
        return None
        
            
# ******************************************************************************


class PropertyWriter:
    def __init__(self, app: BACeeApp):
        self.app = app

    async def write_property(self, device_id, obj_id, prop_id, value, priority=None):
        """Writes a value to a BACnet property and updates the cache."""
        try:
            # ... (Type conversion logic, same as before)
            request = WritePropertyRequest(
                objectIdentifier=obj_id,
                propertyIdentifier=prop_id,
                propertyArrayIndex=None,
                value=ArrayOf(prop_data_type, [value]),
                priority=priority,  # Include priority if provided
            )

            device_info = self.app.deviceInfoCache.get_device_info(device_id)
            if not device_info:
                _logger.error(
                    f"Device with ID {device_id} not found. Cannot write property."
                )
                return

            request.pduDestination = device_info.address
            response = await self.app.request(request)

            if isinstance(response, SimpleAckPDU):
                _logger.info(f"Successfully wrote {value} to {obj_id}.{prop_id}")

                # Update or invalidate cached value
                device_info.set_object_property(obj_id, prop_id, value)
            else:
                _logger.error(
                    f"Failed to write {value} to {obj_id}.{prop_id}: {response}"
                )
        except Exception as e:
            _logger.error(f"Error writing {value} to {obj_id}.{prop_id}: {e}")

    async def write_multiple_properties(self, device_id, obj_id, prop_values, priority=None):
        """Writes multiple properties to a BACnet object using WPM."""
        # ... (Validation logic for property values)

        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if not device_info:
            _logger.error(f"Device with ID {device_id} not found.")
            return

        request = WritePropertyMultipleRequest(
            device_address=device_info.address,
            object_identifier=obj_id,
            values={prop_id: ArrayOf(get_datatype(obj_id[0], prop_id), [value]) for prop_id, value in prop_values.items()}
        )

        try:
            response = await self.app.request(request)
            if not isinstance(response, SimpleAckPDU):
                _logger.error(f"Failed to write multiple properties to device {device_id}, object {obj_id}: {response}")
            else: 
                # Invalidate cache entries for the written properties
                for prop_id in prop_values:
                    cache_key = (device_id, obj_id, prop_id)
                    if cache_key in self.app.property_cache:
                        del self.app.property_cache[cache_key]
        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Communication error with device {device_id}: {e}")
            
            
# ******************************************************************************

    
class BBMD:
    def __init__(self, address, topology_file="network_topology.json"):
        self.address = address
        self.load_configuration()
        self.db_file = db_file
        self.routing_table = {}
        self.topology_file = topology_file
        self.default_bbd_address = None
        self.app = None
        self.is_available = True  # Flag to track BBMD availability
        self.topology_data = None
        self.load_topology()
        self.topology_watcher = asyncio.create_task(self.watch_topology_file())

    def load_configuration(self):
        """Loads BBMD configuration from JSON file and database."""
        try:
            with open(self.topology_file, "r") as f:
                topology_data = json.load(f)
                bbmd_config = next(
                    (bbmd for bbmd in topology_data.get("BBMDs", []) if bbmd.get("name") == bbmd_name),
                    {}
                )
                self.broadcast_address = bbmd_config.get("broadcastAddress", "255.255.255.255")

            # Load any additional information from database (if needed)
            with sqlite3.connect(self.db_file) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT broadcast_address FROM bbmd_settings WHERE address = ?", (self.address,))
                row = cursor.fetchone()
                if row:
                    self.broadcast_address = row[0]  # Override with value from database if found
        
        except (FileNotFoundError, json.JSONDecodeError, sqlite3.Error) as e:
            _log.error(f"Error loading configuration: {e}")
            self.broadcast_address = "255.255.255.255"  # Default to broadcast if there's an error

    def update_configuration(self, new_broadcast_address):
        """Updates the BBMD configuration in both JSON file and database."""
        try:
            # Update JSON file
            with open(self.topology_file, "r+") as f:
                topology_data = json.load(f)
                for bbmd in topology_data.get("BBMDs", []):
                    if bbmd.get("name") == bbmd_name:
                        bbmd["broadcastAddress"] = new_broadcast_address
                        break
                f.seek(0)  # Rewind to the beginning of the file
                json.dump(topology_data, f, indent=4)  # Overwrite with updated data
                f.truncate()

            # Update database
            with sqlite3.connect(self.db_file) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE bbmd_settings SET broadcast_address = ? WHERE address = ?", 
                    (new_broadcast_address, self.address)
                )

            self.broadcast_address = new_broadcast_address
            _log.info(f"BBMD configuration updated: {self.address} - broadcastAddress: {new_broadcast_address}")

        except (FileNotFoundError, json.JSONDecodeError, sqlite3.Error) as e:
            _log.error(f"Error updating configuration: {e}")
    
    async def discover_bbmds(self):
        """Discovers available BBMDs on the network."""
        bbmds = []
        try:
            who_is = WhoIsRequest()
            who_is.pduDestination = Address(BROADCAST_ADDRESS)
            self.app.request(who_is)  # Broadcast WhoIs
            # Use an asyncio.Event to wait for responses for a certain duration.
            await asyncio.sleep(5)  # Wait for IAm responses (adjust as needed)

            for device in self.app.deviceInfoCache.get_device_infos():
                if device.isBBMD:
                    _logger.debug(f"Found BBMD: {device.address}")
                    bbmds.append(device)
        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Error discovering BBMDs: {e}")
        return bbmds
        
    async def select_bbmd(self, bbmds):
        """Selects a BBMD for routing based on your preferred strategy."""
        # Implement your selection logic here (e.g., round-robin, proximity, etc.)
        if not bbmds:
            return None

        return bbmds[0]  # Example: Select the first BBMD found

    def load_topology(self):
        """Loads network topology and BBMDs from JSON file."""
        try:
            with open(self.topology_file, "r") as f:
                self.topology_data = json.load(f)

            # Load BBMDs from JSON (assuming an array of addresses)
            bbmd_addresses = self.topology_data.get("bbmds", [])
            self.bbmds = [Address(addr) for addr in bbmd_addresses]
        
        except (FileNotFoundError, json.JSONDecodeError) as e:
            _logger.error(f"Error loading network topology: {e}")


    async def watch_topology_file(self):
        """Asynchronously monitors the topology file for changes and reloads it."""
        last_modified = os.path.getmtime(self.topology_file)
        while True:
            try:
                current_modified = os.path.getmtime(self.topology_file)
                if current_modified > last_modified:
                    _logger.info("Topology file changed, reloading...")
                    self.load_topology()
                    last_modified = current_modified

                # Check for new devices that need to be subscribed to
                for sub_info in self.topology_data.get("subscriptions", []):
                    device_id = sub_info["device_id"]
                    obj_id = (sub_info["object_type"], sub_info["object_instance"])
                    property_identifier = sub_info["property_identifier"]
                    subscription_key = (device_id, obj_id, property_identifier)

                    if device_id in self.app.discovered_devices and subscription_key not in self.app.subscriptions:
                        try:
                            self.validate_object_and_property(obj_id, property_identifier)
                            subscription = Subscription(device_id, obj_id, property_identifier)
                            subscription.alarms = sub_info.get("alarms", [])
                            self.app.subscriptions[subscription_key] = subscription
                            await self.app.subscribe_cov(subscription)
                        except ValueError as e:
                            _logger.error(f"Invalid subscription: {e}")

            except FileNotFoundError:
                _logger.warning("Topology file not found. Will retry in 5 seconds.")
            await asyncio.sleep(5)  # Check for changes every 5 seconds

    async def request_routing_table(self, destination_address):
        """
        Requests the routing table from the BBMD at the specified destination address.
        """
        _log.debug(f"Requesting routing table from BBMD at {destination_address}")  # Add a debug log

        try:
            # Create a request for the routing table
            request = ReadPropertyRequest(
                objectIdentifier=('device', self.device_id),
                propertyIdentifier='routingTable'
            )
            request.pduDestination = Address(destination_address)

            # Send the request and wait for the response
            try:
                response = await self.app.bacnet_stack.request(request, timeout=5)
            except TimeoutError as e:
                _log.error(f"Timeout error while requesting routing table: {e}")  # Log timeout error
                return None

            # Handle the response
            if isinstance(response, ReadPropertyACK):
                # Extract and return the routing table
                try:
                    routing_table = response.propertyValue.cast_out(SequenceOf(SequenceOf(Unsigned)))
                    _log.debug(f"Received routing table: {routing_table}")
                    return routing_table
                except Exception as e:  # Catch more specific exception if possible
                    _log.error(f"Error extracting routing table data: {e}")
                    return None
            else:
                _log.warning(f"Unexpected response type: {type(response)}")  # Log unexpected response
                return None
        except BACpypesError as e:
            _log.error(f"BACpypes error requesting routing table: {e}")  # Log BACpypes error
            return None


    def get_destination_address(self, device_id):
        """Determines the destination address for a device ID, considering the routing table, 
        default BBD, and BBMD availability.
        """
        device_network = device_id // 1000  # Assuming network number is the first 3 digits of the device ID
        bbd_address = self.routing_table.get(device_network)  # Try to find a BBD for the device's network

        if bbd_address:
            _logger.debug(f"Using BBD address {bbd_address} for device {device_id}")
            return bbd_address
        elif self.default_bbd_address:
            _logger.warning(f"No BBD found for device {device_id}, using default BBD {self.default_bbd_address}")
            return self.default_bbd_address
        elif self.is_available:  # If BBMD is available but no route found
            _logger.warning(f"No BBD found for device {device_id}, using BBMD address {self.address}")
            return self.address  # Use the BBMD's address as a fallback
        else:
            _logger.warning("BBMD unavailable. Using local broadcast as a fallback.")
            return "255.255.255.255/24"  # Local broadcast address

    async def register_foreign_device(self):
        """Registers the local device as a foreign device with all discovered BBMDs."""
        for bbmd in await self.discover_bbmds():
            request = RegisterForeignDeviceRequest(
                foreignDeviceAddress=self.app.localAddress,
                ttl=Unsigned(300),  # 5-minute TTL
            )
            request.pduDestination = bbmd.address
            try:
                response = await self.app.request(request)
                if isinstance(response, SimpleAckPDU):
                    _logger.info(f"Successfully registered as a foreign device with BBMD at {bbmd.address}")
                else:
                    _logger.error(f"Failed to register as a foreign device with BBMD at {bbmd.address}: {response}")
            except (CommunicationError, TimeoutError):
                _logger.error(f"BBMD communication error during registration with {bbmd.address}")


    async def update_routing_table(self, app: "BACeeApp"):
        """Requests and updates the BBMD's routing table."""
        _logger.info("Updating routing table from BBMD")

        request = ReadPropertyRequest(
            objectIdentifier=('device', 1),  
            propertyIdentifier='routingTable'
        )
        request.pduDestination = self.address  # Set destination to BBMD address

        try:
            response = await app.request(request)
            if not isinstance(response, ReadPropertyACK):
                raise BACpypesError(f"Unexpected response type from BBMD: {type(response)}")

            # ... (Rest of the table parsing and update logic remains the same)
        
        except (CommunicationError, TimeoutError) as comm_err:
            _logger.error(f"Failed to communicate with BBMD: {comm_err}")
            self.is_available = False  
        except BACpypesError as e:
            _logger.error(f"Error requesting or parsing routing table: {e}")


# ******************************************************************************


class AlarmManager:
    def __init__(self, app: BIPSimpleApplication):
        self.app = app
        self.active_alarms = {}
        self.acknowledged_alarms = set()
        self.reminder_interval = 60  # Seconds
        self.silenced_alarms = {}  # Key: alarm_key, Value: silence end timestamp
        self.flood_detection_window = 60  # Seconds
        self.flood_threshold = 10
        self.alarm_counts = defaultdict(lambda: defaultdict(int))
        self.alarm_flood_active = defaultdict(lambda: False)
        self.load_silenced_alarms() 
               
    async def handle_cov_notification(self, property_identifier, property_value, subscription):
        """Handles incoming COV notifications and manages alarms."""

        obj_id = subscription.obj_id
        prop_id = subscription.prop_id
        device_id = subscription.device_id

        # Get the most recent values for trend analysis
        history = self.app.cov_history.get(obj_id, {}).get(prop_id, [])
        recent_values = history[-10:]  # Get last 10 or all if less

        # Change Filtering
        if subscription.change_filter:
            # Check for change filter values
            if recent_values:
                previous_value = recent_values[-1][1]
                if abs(property_value - previous_value) < subscription.change_filter:
                    return  # Skip if the change is below the filter threshold
            else:
                # If no previous value, treat it as a change and don't return
                pass

        # Alarm Flood Detection
        alarm_key = (device_id, obj_id, prop_id)
        if not self.is_alarm_silenced(alarm_key):
            await self.detect_alarm_flood(alarm_key)

        # Alarm Logic (Only if not in alarm flood)
        if not self.is_alarm_flood_active(device_id):
            for alarm in subscription.alarms:
                alarm_type = alarm["type"]
                threshold = alarm["threshold"]
                severity = alarm["severity"]
                priority = alarm.get("priority")

                full_alarm_key = (*alarm_key, alarm_type)
                if self._should_trigger_alarm(alarm_type, property_value, threshold):
                    await self.trigger_alarm(
                        device_id,
                        obj_id,
                        prop_id,
                        f"{alarm_type.capitalize()} {property_identifier}",
                        property_value,
                        priority,
                        severity=severity,
                        history=recent_values,
                    )
                elif full_alarm_key in self.active_alarms:
                    await self.clear_alarm(*full_alarm_key)

        # Anomaly Detection (Using Z-Score)
        if len(recent_values) >= 2:  # Ensure enough data for Z-score calculation
            timestamps, values = zip(*recent_values)
            z_scores = (np.array(values) - np.mean(values)) / np.std(values)
            if any(abs(z) > 2 for z in z_scores):  # Check if any z-score is above threshold
                await self.trigger_alarm(
                    device_id,
                    obj_id,
                    prop_id,
                    "Anomaly Detected", 
                    values[-1],  # Latest anomalous value
                    priority=None,
                    z_score=z_scores[-1],  # Latest Z-score
                    severity="minor",
                )


    async def trigger_alarm(self, device_id, obj_id, prop_id, alarm_type, alarm_value, priority=None, z_score=None, severity="medium", history=None):
        """Triggers an alarm, stores it, and sends a notification."""

        alarm_key = (device_id, obj_id, prop_id, alarm_type)

        # Check if the alarm is already active and of the same type
        existing_alarm = self.active_alarms.get(alarm_key)
        if existing_alarm:
            # Check for severity upgrade or value change
            if existing_alarm["severity"] != severity or existing_alarm["alarm_value"] != alarm_value:
                logger.info(f"Updating active alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}: Value={alarm_value}, Z-score={z_score}, Severity={severity}")
                existing_alarm.update({
                    "timestamp": time.time(),
                    "alarm_value": alarm_value,
                    "z_score": z_score,
                    "severity": severity,
                })

                # Save updated alarm to DB
                self.app.save_alarm_to_db(*alarm_key, alarm_value, z_score, existing_alarm["is_anomaly"])
            else:
                logger.debug(f"Alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id} already active and unchanged.")
                return  # No need to trigger again
        else:
            # Create and log a new alarm entry
            logger.info(f"Triggering alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}: Value={alarm_value}, Z-score={z_score}")

            self.active_alarms[alarm_key] = {
                "timestamp": time.time(),
                "alarm_type": alarm_type,
                "alarm_value": alarm_value,
                "priority": priority,
                "z_score": z_score,
                "severity": severity,
                "is_anomaly": alarm_type == "Anomaly",
            }
            self.app.save_alarm_to_db(*alarm_key, alarm_value, z_score, self.active_alarms[alarm_key]["is_anomaly"])

        # Send notification (including potential trend information)
        await self.send_alarm_notification(alarm_key, history=history)  

        # Escalation for new or upgraded critical alarms
        if severity == "critical" and (existing_alarm is None or existing_alarm["severity"] != "critical"):
            asyncio.create_task(self.escalate_alarm(alarm_key))

    async def escalate_alarm(self, alarm_key):
        """Escalates an alarm if not acknowledged within the specified timeframe."""
        try:
            await asyncio.sleep(900)  # Wait for 15 minutes

            # Check if the alarm is still active and not acknowledged
            if alarm_key in self.active_alarms and alarm_key not in self.acknowledged_alarms:
                logger.info(f"Escalating alarm {alarm_key} to level 2")
                await self.send_alarm_notification(alarm_key, escalation_level=2)
            else:
                logger.info(f"Alarm {alarm_key} already acknowledged or cleared. No escalation needed.")
        except asyncio.CancelledError:
            logger.info(f"Escalation task for alarm {alarm_key} cancelled.")


    async def send_alarm_notification(self, alarm_key, escalation_level=1):
        """Sends an alarm notification with escalation level and alarm details."""
        device_id, obj_id, prop_id, alarm_type = alarm_key
        alarm_data = self.active_alarms[alarm_key]  # Get alarm data from the dictionary

        # Get device details from app.deviceInfoCache
        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if device_info is None:
            _logger.error(
                f"Device with ID {device_id} not found. Cannot send alarm notification."
            )
            return

        # Determine recipients based on escalation level
        if escalation_level == 1:
            recipients = ["primary_contact@example.com"]  # Replace with actual emails
        elif escalation_level == 2:
            recipients = ["secondary_contact@example.com", "manager@example.com"]
        else:
            recipients = ["admin@example.com"]  # Highest escalation level

        # Construct the notification message
        message_content = f"""
        BACnet Alarm Notification

        Severity: {alarm_data['severity']}
        Alarm Type: {alarm_type}
        Device: {device_info.device_name} ({device_id})
        Object: {obj_id}
        Property: {prop_id}
        Value: {alarm_data['alarm_value']}
        Timestamp: {datetime.fromtimestamp(alarm_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}
        Priority: {alarm_data.get('priority', 'Unknown')}
        """

        # Send email notifications to the specified recipients
        for recipient_email in recipients:
            try:
                 await self.app.send_email_notification(message_content, recipient_email)
                _logger.info(f"Alarm notification email sent to {recipient_email}")
            except Exception as e:
                _logger.error(
                    f"Failed to send alarm notification email to {recipient_email}: {e}"
                )

     def _get_notification_recipients(self, escalation_level):
        """Determines notification recipients based on the escalation level."""
        recipients = {
            1: ["primary_contact@example.com"],
            2: ["secondary_contact@example.com", "manager@example.com"],
        }
        return recipients.get(escalation_level, ["admin@example.com"])  # Default to admin for unknown levels

    def _format_alarm_message(self, alarm_data, device_info, history=None):
        """Formats the alarm message content."""

        # Ensure that alarm_data dictionary has necessary keys
        try:
            message_content = f"""
            BACnet Alarm Notification

            Severity: {alarm_data['severity']}
            Alarm Type: {alarm_data['alarm_type']}
            Device: {device_info.device_name} ({device_info.device_identifier[1]})
            Object: {alarm_data['object_id']}
            Property: {alarm_data['property_id']}
            Value: {alarm_data['alarm_value']}
            Timestamp: {datetime.fromtimestamp(alarm_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}
            Priority: {alarm_data.get('priority', 'Unknown')}
            """
        except KeyError as e:
            logger.error(f"Error formatting alarm message: Missing key in alarm_data - {e}")
            return "Error: Invalid alarm data"

        if history and alarm_data.get('is_anomaly'):  # Safe access with .get()
            # Add trend information to anomaly alarm message
            trend_info = self._calculate_trend(history)
            message_content += f"\nRecent Trend: {trend_info}"

        return message_content

    def _calculate_trend(self, history, threshold=0.05):  # Add a threshold parameter
        """Calculates a trend from the given history based on a percentage change threshold."""

        if len(history) < 2:
            return "Insufficient data"

        timestamps, values = zip(*history)
        start_value = values[0]
        end_value = values[-1]
        percentage_change = (end_value - start_value) / abs(start_value) if start_value != 0 else 0

        if percentage_change > threshold:
            return "Increasing rapidly"
        elif percentage_change < -threshold:
            return "Decreasing rapidly"
        elif percentage_change > 0:
            return "Increasing"
        elif percentage_change < 0:
           return "Decreasing"
        else:
            return "Stable" 
            
    # In the trigger_alarm function
    # await self.send_alarm_notification(alarm_key)  # Initial notification (level 1)

    # In the escalate_alarm function
    # await self.send_alarm_notification(alarm_key, escalation_level=2)  # Escalation (level 2)

    async def send_email_notification(self, message_content, recipient_email):
        """Sends an email notification using SMTP (e.g., Gmail)."""
    
        sender_email = os.environ.get("EMAIL_SENDER")  # Load from environment variable
        password = os.environ.get("EMAIL_PASSWORD")  # Load from environment variable

        if not sender_email or not password:
            logger.error("Email credentials not found in environment variables. Cannot send notification.")
            return  # Don't proceed if credentials are missing

        message = MIMEMultipart("alternative")
        message["Subject"] = "BACnet Alarm Notification"
        message["From"] = sender_email
        message["To"] = recipient_email
    
        # Include plain text and HTML versions of the message
        text_part = MIMEText(message_content, "plain")
        # Optionally, create an HTML version with better formatting
        # html_part = MIMEText(f"<html><body>{message_content}</body></html>", "html") 

        message.attach(text_part)
        # message.attach(html_part) 

        try:
            # Use a context manager for automatic cleanup
            async with AioSmtplib.SMTP_SSL("smtp.gmail.com", 465) as server:  # Or your SMTP server
                await server.login(sender_email, password)
                await server.send_message(message) 
        
            logger.info(f"Alarm notification email sent to {recipient_email}")
         except Exception as e:
            logger.error(f"Failed to send alarm notification email to {recipient_email}: {e}")


    async def clear_alarm(self, device_id, obj_id, prop_id, alarm_type):
        """Clears a previously triggered alarm."""

        alarm_key = (device_id, obj_id, prop_id, alarm_type)

        # Remove from acknowledged alarms (if present)
        if alarm_key in self.acknowledged_alarms:
            _logger.info(f"Clearing acknowledged alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}")
            self.acknowledged_alarms.remove(alarm_key)
        else:
            # Check for alarm in active alarms
            if alarm_key not in self.active_alarms:
                logger.warning(f"Tried to clear a non-existent alarm: {alarm_key}")
                return  # Exit early if alarm is not found

        # Remove from active alarms
        del self.active_alarms[alarm_key]

        # Update the database to mark the alarm as cleared
        try:
            with self.app.db_conn:  # Use a context manager for automatic transaction handling
                cursor = self.app.db_conn.cursor()
                cursor.execute(
                    "UPDATE alarms SET acknowledged = 1, timestamp = ? WHERE device_id = ? AND object_id = ? AND property_id = ? AND alarm_type = ?",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), device_id[1], str(obj_id), prop_id, alarm_type)
                )
        except sqlite3.Error as e:
            logger.error(f"Error clearing alarm in database: {e}")

    async def manage_alarms(self):
        """Periodically checks active alarms and sends reminders if they persist."""
        while True:
            current_time = time.time()  # Get the current time once per loop
            for alarm_key, alarm_data in list(self.active_alarms.items()):  
                if current_time - alarm_data['timestamp'] > self.reminder_interval:
                    device_id, obj_id, prop_id, alarm_type = alarm_key
                    logger.warning(
                        f"Alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id} is still active. Sending reminder."
                    )

                    # (Option 1) Send reminder directly
                    await self.send_reminder_notification(alarm_key)  

                    # (Option 2) Create a task for sending the reminder
                    # asyncio.create_task(self.send_reminder_notification(alarm_key)) 
            
            await asyncio.sleep(self.reminder_interval)
    
    async def send_reminder_notification(self, alarm_key):
        """Sends a reminder notification for the given alarm."""
        device_id, obj_id, prop_id, alarm_type = alarm_key
        # Implementation for sending the reminder notification

    async def acknowledge_alarm(self, alarm_key):
        """Acknowledges an alarm, moving it from active to acknowledged and updates the database."""

        try:
            if alarm_key in self.active_alarms:
                logger.info(f"Acknowledging alarm {alarm_key}")

                # Move alarm to acknowledged set
                self.acknowledged_alarms.add(alarm_key)

                # Remove from active alarms
                del self.active_alarms[alarm_key]

                # Update the database to mark the alarm as acknowledged
                await self.update_alarm_acknowledgment_in_db(alarm_key, acknowledged=True)

                # Additional actions on acknowledgment (e.g., notification) can be added here
            else:
                logger.warning(f"Alarm {alarm_key} not found in active alarms. Cannot acknowledge.")
        except Exception as e:
            logger.exception(f"An unexpected error occurred while acknowledging alarm {alarm_key}: {e}")
    

    async def update_alarm_acknowledgment_in_db(self, alarm_key, acknowledged):
        """Updates the acknowledgment status of an alarm in the database."""
        try:
            with self.app.db_conn:  # Use context manager for automatic transactions
                cursor = self.app.db_conn.cursor()
                device_id, obj_id, prop_id, alarm_type = alarm_key
                cursor.execute(
                    "UPDATE alarms SET acknowledged = ? WHERE device_id = ? AND object_id = ? AND property_id = ? AND alarm_type = ?",
                    (acknowledged, device_id, str(obj_id), prop_id, alarm_type)
                )  
        except sqlite3.Error as e:
            logger.error(f"Error updating alarm acknowledgment in database: {e.args[0]}")

    def save_cov_notification_to_db(self, device_id, obj_id, prop_id, value):
        try:
            device_id = int(device_id[1])  # Extract device instance from tuple
            cursor = self.app.db_conn.cursor()
            cursor.execute(
                "INSERT INTO cov_notifications (timestamp, device_id, object_id, property_id, value) VALUES (?, ?, ?, ?, ?)",
                (time.time(), device_id, str(obj_id), prop_id, str(value)),  # Parameterized query
            )
            self.app.db_conn.commit()
            _logger.debug(
                f"COV notification saved to database: {obj_id}.{prop_id} = {value} (Device {device_id})"
            )

        except sqlite3.Error as e:
            _logger.error(f"Error saving COV notification to database: {e}")
            self.app.db_conn.rollback()  # Rollback on error
            
    def save_alarm_to_db(self, device_id, obj_id, prop_id, alarm_type, alarm_value, z_score=None, is_anomaly=False):
        try:
            device_id = int(device_id[1])
            z_score = float(z_score) if z_score is not None else None
            is_anomaly = int(is_anomaly)

            cursor = self.app.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO alarms 
                (timestamp, device_id, object_id, property_id, alarm_type, alarm_value, z_score, is_anomaly)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), device_id, str(obj_id), prop_id, alarm_type, str(alarm_value), z_score, is_anomaly)  # Parameterized query
            )
            self.app.db_conn.commit()
            _logger.debug(f"Alarm saved to database: {alarm_type} for {obj_id}.{prop_id} on device {device_id}")
        except sqlite3.Error as e:
            _logger.error(f"Error saving alarm to database: {e}")
            self.app.db_conn.rollback()

    def load_silenced_alarms(self):
        """Loads silenced alarms from the database into memory."""
        try:
            cursor = self.app.db_conn.cursor()
            cursor.execute("SELECT device_id, object_id, property_id, alarm_type, silence_end_time FROM silenced_alarms")
            rows = cursor.fetchall()
            for row in rows:
                device_id, object_id, property_id, alarm_type, silence_end_time = row
                alarm_key = (device_id, tuple(map(int, object_id[1:-1].split(", "))), property_id, alarm_type)
                self.silenced_alarms[alarm_key] = silence_end_time
            _logger.info(f"Silenced Alarms Successfully Loaded: {self.silenced_alarms}")
        except sqlite3.Error as e:
            _logger.error(f"Error loading silenced alarms from database: {e}")


    def is_alarm_silenced(self, alarm_key):
        """Checks if an alarm is currently silenced."""
        silence_end_time = self.silenced_alarms.get(alarm_key)
        return silence_end_time is not None and time.time() < silence_end_time

    def detect_alarm_flood(self, alarm_key):
        """Detects and handles alarm floods."""
        device_id = alarm_key[0]
        now = time.time()
        time_window_start = now - (now % self.flood_detection_window)

        self.alarm_counts[device_id][time_window_start] += 1
        if self.alarm_counts[device_id][time_window_start] > self.flood_threshold and not self.alarm_flood_active[device_id]:
            _logger.warning(f"Alarm flood detected on device {device_id}!")
            self.alarm_flood_active[device_id] = True

            # Send flood notification (modify this to your preferred method)
            message_content = f"Alarm flood detected on device {device_id}!"
            asyncio.create_task(self.app.send_email_notification(message_content, "admin@example.com")) 
            # Schedule flood deactivation
            asyncio.create_task(self.deactivate_alarm_flood(device_id))

    async def deactivate_alarm_flood(self, device_id):
        """Deactivates the alarm flood after the suppression period."""
        await asyncio.sleep(self.flood_detection_window * 2)  # Suppression period (2 time windows)
        self.alarm_flood_active[device_id] = False
        _logger.info(f"Alarm flood deactivated for device {device_id}")

    def is_alarm_flood_active(self, device_id):
        """Checks if an alarm flood is currently active for the device."""
        return self.alarm_flood_active.get(device_id, False)

    def silence_alarm(self, device_id, obj_id, prop_id, alarm_type, duration):
        """Silences an alarm."""
        alarm_key = (device_id, obj_id, prop_id, alarm_type)
        silence_end_time = time.time() + duration
        self.silenced_alarms[alarm_key] = silence_end_time

        # Save to the database
        try:
            cursor = self.app.db_conn.cursor()
            cursor.execute(
                "INSERT INTO silenced_alarms (device_id, object_id, property_id, alarm_type, silence_end_time) VALUES (?, ?, ?, ?, ?)",
                (device_id, str(obj_id), prop_id, alarm_type, silence_end_time),  # Parameterized query
            )
            self.app.db_conn.commit()
            _logger.info(
                f"Alarm {alarm_key} silenced for {duration} seconds and stored in the database."
            )
        except sqlite3.Error as e:
            _logger.error(f"Error silencing alarm in database: {e}")
            self.app.db_conn.rollback()

                              
# ******************************************************************************


class BACeeApp(BIPSimpleApplication, ChangeOfValueServices):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._log = ModuleLogger(globals())
        self.subscriptions = {}
        
        # Create the LocalDeviceObject instance
        self.this_device = LocalDeviceObject(
            objectName=DEVICE_NAME,
            objectIdentifier=('device', DEVICE_ID),
            maxApduLengthAccepted=1024,  # Set maximum APDU length (adjust as needed)
            segmentationSupported='segmentedBoth',  # Specify segmentation support
        )
        
        # Create and bind the Network Service Elements
        self.nsap = NetworkServiceAccessPoint()
        self.nse = NetworkServiceElement()
        bind(self.nse, self.nsap)
        bind(self, self.nse)

        # Initialize the DeviceInfoCache
        self.deviceInfoCache = DeviceInfoCache(self.this_device)

        # Simple BBMD configuration (if applicable)
        if BBMD_ADDRESS:
            # Check if BBMD address is different from the local address
            if BBMD_ADDRESS != Address(LOCAL_ADDRESS):
                settings().add_address_binding(
                    Address(LOCAL_ADDRESS), bind(BIPForeign(BBMD_ADDRESS), UDPMultiplexer()),
                )
                self.bbmd = BBMD(Address(BBMD_ADDRESS))
                self.bbmd.app = self
            else:
                _logger.warning("BBMD address is the same as the local address. BBMD configuration skipped.")
        else:
            _logger.warning("BBMD address not provided in configuration. BBMD configuration skipped.")

        # Other initializations...
        self.acknowledged_alarms = set()
        self.active_alarms = {}
        self.cov_history = defaultdict(lambda: defaultdict(list))
        self.reminder_interval = 60  # Send reminders every 60 seconds

        self.db_conn = sqlite3.connect('bacee.db')
        self.create_tables()

        self.property_reader = PropertyReader(self)
        self.property_writer = PropertyWriter(self)
        self.alarm_manager = AlarmManager(self)
        self.trend_analyzer = TrendAnalyzer(self)


    def create_tables(self):
        """Creates the necessary tables in the database."""
        cursor = self.db_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alarms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, 
                device_id INTEGER, 
                object_id TEXT, 
                property_id TEXT, 
                alarm_type TEXT,
                alarm_value TEXT,
                z_score REAL, 
                is_anomaly INTEGER DEFAULT 0, 
                acknowledged INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cov_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL, 
                device_id INTEGER, 
                object_id TEXT, 
                property_id TEXT, 
                value TEXT
            )
        ''')
        
        # Create the silenced_alarms table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS silenced_alarms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER,
                object_id TEXT,
                property_id TEXT,
                alarm_type TEXT,
                silence_end_time REAL
            )
        ''')       
        
        # Add registered devices table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registered_devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER UNIQUE,
                objects_properties TEXT
            )
        ''')
        self.db_conn.commit()

    async def start(self):
        """Starts the BACnet application, discovers and registers with BBMDs (if applicable)."""

        # Existing start-up logic
        try:
            await self.async_start()
        except Exception as err:
            self._log.error("failed to start: %r", err)
            return

        _logger.info(f"BACnet application started for device: {self.localDevice.objectName}")
        
        # Discover and register with BBMDs if necessary
        if BBMD_ADDRESS:
            self.bbmd = BBMD(Address(BBMD_ADDRESS))  # Create BBMD object
            
            discovered_bbmds = await self.bbmd.discover_bbmds()
            
            # Select a BBMD to use
            self.bbmd.app = self
            selected_bbmd = await self.bbmd.select_bbmd(discovered_bbmds)
            
            if selected_bbmd:
                _logger.info(f"Selected BBMD: {selected_bbmd.address}")
                self.bbmd.address = selected_bbmd.address
                # Register with selected BBMD 
                await self.bbmd.register_foreign_device()  

        # Start who-is to find other devices
        who_is = WhoIsRequest()
        who_is.pduDestination = Address(BROADCAST_ADDRESS)
        self.request(who_is)
        
        await asyncio.sleep(5)  # Sleep for 5 seconds to allow responses
        
        # Initialize subscriptions after device discovery
        self.bbmd.load_topology()

        # Load registered devices from the database
        await self.load_registered_devices()

        _logger.debug("running")

    async def shutdown(self):
        """Gracefully shuts down the BACnet application."""
        logger.info("Shutting down BACnet application...")

        # Unsubscribe from all COVs
        for subscription in self.subscriptions.keys():
            await self.unsubscribe_cov(subscription.device_id, subscription.obj_id, subscription.prop_id)

        # Cancel any pending tasks
        for task in asyncio.all_tasks():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Close the database connection
        self.db_conn.close()
        
        # Stop the BACnet application
        stop()
          
    async def load_registered_devices(self):
        """Loads registered device information from the database."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT device_id, objects_properties FROM registered_devices")
            rows = cursor.fetchall()
            for row in rows:
                device_id, objects_properties = row
                objects_properties = json.loads(objects_properties)  # Parse JSON data
                self.registered_devices[device_id] = objects_properties
            _logger.info(f"Registered devices loaded from database: {self.registered_devices}")
        except sqlite3.Error as e:
            _logger.error(f"Error loading registered devices from database: {e}")

    def save_registered_device(self, device_id, objects_properties):
        """Saves registered device information to the database."""
        try:
            objects_properties_json = json.dumps(objects_properties)
            cursor = self.db_conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO registered_devices (device_id, objects_properties) VALUES (?, ?)",
                (device_id, objects_properties_json),
            )
            self.db_conn.commit()
            _logger.info(f"Registered device saved to database: {device_id}")
        except sqlite3.Error as e:
            _logger.error(f"Error saving registered device to database: {e}")
            self.db_conn.rollback()
        
    async def do_IAmRequest(self, apdu: IAmRequest):
        """Handles incoming I-Am requests, including device registration."""

        self._log.debug(f"Received I-Am Request from {apdu.pduSource}")

        # Add or Update Device in Cache
        device_info = self.deviceInfoCache.get_device_info(apdu.pduSource)
        device_info.update_device_info(apdu)
        self.deviceInfoCache.add_device_info(device_info)
        device_id = device_info.device_identifier[1]  # Extract device ID
        
        # Check for Registration Information
        registration_data = apdu.vendorProprietaryValue  # Assuming registration data is here (adjust if needed)
        if registration_data and self.validate_registration(registration_data, device_id):
            _logger.info(f"Device {device_id} registered successfully.")

            # Store registration info in JSON file and database
            self.registered_devices[device_id] = registration_data.get('objects_properties', {})
            self.save_registered_devices()

            # Subscribe to properties from registration data
            for obj_id, prop_ids in registration_data.get('objects_properties', {}).items():
                obj_type, obj_instance = obj_id
                for prop_id in prop_ids:
                    subscription = Subscription(device_id, (obj_type, obj_instance), prop_id)
                    subscription.alarms = []  # Assuming no alarms for registered devices by default
                    self.subscriptions[(device_id, (obj_type, obj_instance), prop_id)] = subscription
                    await self.subscribe_cov(subscription)
        else:
            _logger.warning(f"Invalid or inconsistent registration data from device {device_id}: {registration_data}")


    def validate_registration(self, registration_data, device_id):
        """Validates the registration data against both JSON and database, prioritizing the database."""

        # 1. Validate Object and Property Identifiers
        for obj_id, prop_ids in registration_data.get('objects_properties', {}).items():
            obj_type, obj_instance = obj_id
            if not self.validate_object_and_property((obj_type, obj_instance), prop_ids):
                _logger.warning(f"Invalid object type, instance, or property in registration data for device {device_id}")
                return False

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT objects_properties FROM registered_devices WHERE device_id = ?",
                (device_id,),
            )
            row = cursor.fetchone()
            if row:
                db_data = json.loads(row[0])  # Parse JSON from database
                if db_data != registration_data.get('objects_properties', {}):
                    _logger.warning(f"Inconsistent registration data in database for device {device_id}. Updating JSON file.")
                    # Update the JSON file to match the database (optional)
                    self.registered_devices[device_id] = db_data
                    self.save_registered_devices()
                return True  # Valid if it matches the database (even if not the JSON)
            else:
                # No entry in the database, consider it valid
                return True  
        except sqlite3.Error as e:
            _logger.error(f"Error querying registered devices database: {e}")

        # If database query fails, check the JSON file
        if os.path.exists(self.registered_devices_file):
            try:
                with open(self.registered_devices_file, "r") as f:
                    json_data = json.load(f)
                return json_data.get(str(device_id), None) == registration_data.get('objects_properties', {})
            except (FileNotFoundError, json.JSONDecodeError) as e:
                _logger.error(f"Error reading registered devices file: {e}")

        # If both database and JSON checks fail, consider it invalid
        return False  

    def validate_object_and_property(self, obj_id, prop_ids):
        """Helper function to validate object type, instance, and properties."""
        try:
            obj_type, obj_instance = obj_id
            # Ensure object type and instance are valid
            obj_class = get_object_class(obj_type)
            if not obj_class:
                _logger.error(f"Unknown object type: {obj_type}")
                return False

            # Ensure all properties are valid for the object type
            for prop_id in prop_ids:
                if prop_id not in obj_class.properties:
                    _logger.error(f"Unknown property: {prop_id} for object type: {obj_type}")
                    return False

            return True
        except Exception as e:
            _logger.error(f"Error validating object and property: {e}")
            return False

    async def do_WhoIsRequest(self, apdu: WhoIsRequest) -> None:
        """Responds to Who-Is requests to announce the local device."""
        _logger.debug("Received Who-Is Request")

        # 1. Check if Local Device is Initialized
        if self.localDevice is None or self.localDevice.objectName is None or self.localAddress is None:
            _logger.warning("Local device not fully initialized. Skipping Who-Is response.")
            return

        # 2. Extract Range Limits (Optional)
        low_limit = apdu.deviceInstanceRangeLowLimit
        high_limit = apdu.deviceInstanceRangeHighLimit

        # 3. Get Local Device ID
        device_id = self.localDevice.objectIdentifier[1]

        # 4. Respond Within Range (or Unrestricted)
        if (not low_limit and not high_limit) or (low_limit <= device_id <= high_limit):
            self.response(
                IAmRequest(
                    iAmDeviceIdentifier=self.localDevice.objectIdentifier,
                    maxApduLengthAccepted=self.localDevice.maxApduLengthAccepted,
                    segmentationSupported=self.localDevice.segmentationSupported,
                    vendorID=self.localDevice.vendorIdentifier,
                )
            )


    async def discover_devices(self):
        """
        Discovers BACnet devices on the network using either local broadcast or a BBMD.
        """
        device_info_cache = DeviceInfoCache(self.app.bacnet_stack)  # Use DeviceInfoCache
        discovered_devices = []

        def callback(address, device_id):
            """
            Callback function to handle discovered device information.
            """
            device_info_cache.update_device_info(device_id, address)
            _log.info(f"Discovered device: {device_id} at {address}")
            discovered_devices.append(
                {"device_id": device_id, "address": address}
            )

        try:
            for bbmd in self.app.bbmd_manager.bbmds.values():  # Iterate over BBMDs in the manager
                if bbmd.is_available:  # Use the 'is_available' property to check BBMD status
                    _log.info(f"Discovering devices using BBMD at {bbmd.address}")
                    await self.app.bacnet_stack.who_is(
                        remoteStation=bbmd.address
                    )
                    break  # Exit the loop after discovering devices through the first available BBMD
            else:
                _log.info("Discovering devices using local broadcast")
                await self.app.bacnet_stack.who_is()

            # Wait for a short period to allow responses to come in
            await asyncio.sleep(2) 

        except BACpypesError as e:
            _log.error(f"Error during device discovery: {e}")

        finally:
            self.app.deviceInfoCache = device_info_cache  # Update the device info cache
        return discovered_devices

    async def check_writable_properties(self, device_id, object_type, object_instance):
        """Checks writable properties using read_multiple_properties."""

        obj_id = (object_type, object_instance)
        _logger.info(f"Checking writable properties for object {obj_id} on device {device_id}")
  
        try:
            result = await self.property_reader.read_multiple_properties(device_id, obj_id, ['propertyList', 'propertyDescription'])

            if result is None:
                _logger.error(f"Failed to read properties for object {obj_id} on device {device_id}")
                return []

            property_list = [prop.identifier for prop in result['propertyList'][0] if isinstance(prop, PropertyIdentifier)]
            property_descriptions = result['propertyDescription']  # List of PropertyDescription objects

            writable_properties = []
            for prop_id, prop_desc in zip(property_list, property_descriptions):
                if prop_desc.writable:
                    writable_properties.append(prop_id)

            return writable_properties

        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Communication error with device {device_id}: {e}")
        return []


    async def check_property_writable(self, device_id, obj_id, property_identifier):
        """Check if a property is writable by reading its property description."""
        try:
            # 1. Read Property Description
            read_result = await self.property_reader.read_property(device_id, obj_id, 'propertyDescription')
            
            # 2. Handle Errors and Null Results
            if read_result is None or not isinstance(read_result, ReadPropertyACK) or len(read_result.propertyValue) == 0:
                _logger.error(f"Failed to read property description for {obj_id}.{property_identifier} on device {device_id}")
                return None

            # 3. Extract and Check Property Description
            property_description = read_result.propertyValue[0]
            
            # 4. Determine Writability
            if hasattr(property_description, 'writable') and property_description.writable:
                _logger.debug(f"Property {property_identifier} is writable for object {obj_id} on device {device_id}")
                return True  # Property is writable
            else:
                _logger.debug(f"Property {property_identifier} is not writable for object {obj_id} on device {device_id}")
                return False  # Property is not writable

        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Communication error with device {device_id}: {e}")
            return None
        except Exception as e:
            _logger.error(f"Unexpected error checking writability of {obj_id}.{property_identifier} on device {device_id}: {e}")
            return None

    async def get_object_list(self, device_id):
        """Retrieves object list for a device using ReadPropertyMultiple, utilizing the cache."""
        device_info = self.deviceInfoCache.get_device_info(device_id)
        if device_info is None:
            _logger.error(f"Device with ID {device_id} not found.")
            return None
    
        # Check the device cache for the object list
        if device_info.object_list:
            return device_info.object_list 

        try:
            result = await self.property_reader.read_multiple_properties(
                device_id,
                ("device", device_id),
                ["objectList"],
            )
            if result is not None and "objectList" in result:
                object_list = [item.objectIdentifier for item in result["objectList"][0].value]
                # Update DeviceInfoCache
                device_info.object_list = object_list
                return object_list
            else:
                _logger.error(f"Failed to read object list for device {device_id}")
        except (CommunicationError, TimeoutError) as e:
            _logger.error(f"Error reading object list from device {device_id}: {e}")
        return None
    
    async def iter_objects(self, device_id):
        """Asynchronously iterates over all objects in a device."""
        object_list = await self.get_object_list(device_id)
        if object_list:
            for obj_id in object_list:
                yield obj_id

    async def do_ConfirmedCOVNotification(self, apdu: ConfirmedCOVNotificationRequest):
        """Handles incoming COV notifications."""
        try:
            device_id = apdu.initiatingDeviceIdentifier
            device_info = self.deviceInfoCache.get_device_info(device_id)

            if not device_info:
                _logger.warning(f"Received COV notification from unknown device: {device_id}")
                return

            obj_id = apdu.monitoredObjectIdentifier
            prop_id = apdu.monitoredPropertyIdentifier

            # Handle sequence of values if present
            if isinstance(apdu.listOfValues[0].value, SequenceOf):
                values = [element.value for element in apdu.listOfValues[0].value]
            else:
                values = apdu.listOfValues[0].value

            _logger.debug(
                f"Received COV notification from {device_id}: {obj_id}.{prop_id} = {values}"
            )

            # Update COV history
            self.cov_history[obj_id][prop_id].append((time.time(), values))
            self.alarm_manager.save_cov_notification_to_db(device_id, obj_id, prop_id, values)

            # Check if the property change is to the objectList
            if prop_id == "objectList":
                # Invalidate the object list cache for this device
                self.deviceInfoCache.remove_device_info(device_id)  # Remove from cache
                # You might want to rediscover the device's objects here

            # Find subscription based on device_id, obj_id, prop_id
            matching_subscriptions = [
                sub for sub in self.subscriptions.keys() if 
                sub.device_id == device_id and 
                sub.obj_id == obj_id and 
                sub.prop_id == prop_id
            ]

            for subscription in matching_subscriptions:
                await self.alarm_manager.handle_cov_notification(prop_id, values, subscription)
                await socketio.emit('property_update', {'deviceId': subscription.device_id, 'objectId': subscription.obj_id, 'propertyId': prop_id, 'value': values}, to=request.sid)

        except Exception as e:
            _logger.error(
                f"Error processing COV notification for device {device_id}, object {obj_id}, property {prop_id}: {e}"
            )
            
    async def subscribe_cov(self, subscription: Subscription, renew: bool = False, timeout: int = 5):
        """Subscribes or renews a COV subscription."""
        device_info = self.deviceInfoCache.get_device_info(subscription.device_id)
        if not device_info:
            _logger.error(f"Device with ID {subscription.device_id} not found. Cannot subscribe.")
            return

        _logger.info(
            f"{'Renewing' if renew else 'Subscribing to'} COV for "
            f"{subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}"
        )

        try:
            async with asyncio.timeout(timeout):
                if not renew:  # If it's a new subscription
                    async with self.change_of_value(
                        device_info.address,
                        subscription.obj_id,
                        subscription.prop_id,
                        subscription.confirmed_notifications,
                        subscription.lifetime_seconds,
                    ) as scm:
                        subscription.context_manager = scm
                        self.subscriptions[subscription] = scm
                        _logger.info(f"Subscribed to COV for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}")
                else:
                    # Renewal (using the existing context manager)
                    subscription.context_manager.renew_subscription()
                    _logger.info(f"Renewed COV subscription for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}")

                # Handle COV Notifications
                while not self.subscriptions[subscription].is_fini:
                    property_identifier, property_value = await scm.get_value()
                    _logger.info(f"Received COV notification: {subscription.obj_id}.{property_identifier} = {property_value}")
                    await self.alarm_manager.handle_cov_notification(property_identifier, property_value, subscription)
                    await socketio.emit('property_update', {'deviceId': subscription.device_id, 'objectId': subscription.obj_id, 'propertyId': property_identifier, 'value': property_value})

        except asyncio.TimeoutError:
            _logger.warning(f"Subscription timeout for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}")
            # Handle timeout (e.g., retry or mark subscription as inactive)


    async def manage_subscriptions(self):
        """Manages active subscriptions, including renewals."""
        while True:
            for subscription, scm in self.subscriptions.items():  
                if subscription.active and subscription.lifetime_seconds is not None:
                    remaining_lifetime = subscription.lifetime_seconds - (time.time() - subscription.last_renewed_time)
                    if remaining_lifetime < 60:  # Renew a minute before expiration
                        await subscription.renew_subscription(self)
            await asyncio.sleep(60)  # Check every minute (adjust as needed)

    async def unsubscribe_cov(self, device_id, obj_id, prop_id):
        """Unsubscribes from COV notifications."""
        _logger.info(f"Unsubscribing from COV for {obj_id}.{prop_id} on device {device_id}")
        try:
            for sub, scm in self.subscriptions.items():
                if sub.device_id == device_id and sub.obj_id == obj_id and sub.prop_id == prop_id:
                    scm.fini.set()
                    del self.subscriptions[sub]
                    break
            else:
                raise ValueError("Subscription not found")
        except Exception as e:
            _logger.error(f"Error unsubscribing from COV: {e}")
            
    async def request_io(self, request, timeout=5, retries=3):

        if self.bbmd and not request.pduDestination.is_broadcast:
            request.pduDestination = self.bbmd.address  # Route through BBMD if available
            _logger.debug(f"Sending request via BBMD to {request.pduDestination}")
        else:
            request.pduDestination = Address(BROADCAST_ADDRESS)
            _logger.debug(f"Sending request via broadcast to {request.pduDestination}")

        for attempt in range(retries + 1):
            try:
                response = await asyncio.wait_for(self.request(request), timeout)
                if response:
                    _logger.debug(
                        f"Received response from {request.pduDestination}: {response}"
                    )
                    return response  # Successful response, return it
                else:  # No response received within the timeout
                    _logger.warning(
                        f"No response received from {request.pduDestination} in attempt {attempt + 1}/{retries + 1}"
                    )
                    raise BACpypesError("No response received")  # Raise an error for retry
            except asyncio.TimeoutError:
                _logger.warning(
                    f"Timeout error for request to {request.pduDestination}, attempt {attempt + 1} of {retries + 1}. Retrying..."
                )
            except (CommunicationError, BACpypesError) as e:
                _logger.error(f"Error sending request to {request.pduDestination}: {e}")

            # Exponential backoff for retries
            await asyncio.sleep(2 ** attempt)

        _logger.error(
            f"Request to {request.pduDestination} failed after {retries} retries."
        )
        return None  # Indicate failure after all retries



    def do_RouterAvailable(self, apdu):
        """Called when a router becomes available."""
        _logger.info(f"Router available: {apdu.pduSource}")

    def do_RouterUnavailable(self, apdu):
        """Called when a router becomes unavailable."""
        _logger.info(f"Router unavailable: {apdu.pduSource}")

    async def schedule_task(self):
        """Asynchronously executes scheduled tasks."""
        while True:
            try:
                cursor = self.db_conn.cursor()
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current time
                cursor.execute(
                    "SELECT device_id, object_id, property_id, value FROM schedules "
                    "WHERE active = 1 AND scheduled_time <= ?", (now,)
                )

                for row in cursor.fetchall():
                    device_id, obj_id, prop_id, value = row
                    device_id = int(device_id)
                    obj_id = eval(obj_id)  # Convert string to tuple

                    _logger.info(f"Executing scheduled task: {obj_id}.{prop_id} = {value} on device {device_id}")
                    await self.property_writer.write_property(device_id, obj_id, prop_id, value)
                    # update to mark complete or inactive if needed here

            except sqlite3.Error as e:
                _logger.error(f"Error executing scheduled task: {e}")

            # Sleep for one minute
            await asyncio.sleep(60)

# ******************************************************************************


class TrendAnalyzer:
    def __init__(self, app: BACeeApp):
        self.app = app
        self.cov_history = defaultdict(lambda: defaultdict(list))

    async def analyze_trends(self, obj_id, prop_id):
        """Calculates and displays a trend for the given object and property."""
        history = self.app.cov_history.get(obj_id, {}).get(prop_id, [])
        if len(history) < 2:
            _logger.warning("Not enough data for trend analysis.")
            return

        timestamps, values = zip(*history)
        x = np.array(timestamps)
        y = np.array(values)

        # Calculate trend line (linear regression)
        coeffs = np.polyfit(x, y, 1)
        trend_line = np.poly1d(coeffs)

        # Plot
        plt.figure()
        plt.plot(x, y, 'o', label='Actual Values')
        plt.plot(x, trend_line(x), 'r-', label='Trend Line')
        plt.xlabel('Timestamp')
        plt.ylabel(prop_id)
        plt.title(f"Trend for {obj_id}.{prop_id}")
        plt.legend()
        plt.show()

# ******************************************************************************


# Helper functions for CLI interactions

async def async_input(prompt: str) -> str:
    """Get input from the user asynchronously."""
    # Wrap input() in an executor to run it in a separate thread
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)
    
async def handle_create_subscription(app):
    """Handles the process of creating a subscription."""
    # Display discovered devices for user selection
    _logger.info("\nDiscovered Devices:")
    for idx, device in enumerate(app.discovered_devices.values()):
        _logger.info(f"{idx + 1}. Device ID: {device.device_id}, Name: {device.device_name}, Address: {device.device_address}")

    while True:
        try:
            device_index = int(await async_input("Select device by index: ")) - 1
            if 0 <= device_index < len(app.discovered_devices):
                selected_device = list(app.discovered_devices.values())[device_index]
                break
            else:
                _logger.error("Invalid device index. Please enter a valid number.")
        except ValueError:
            _logger.error("Invalid input. Please enter a number.")

    # Display object list for user selection
    object_list = await app.iter_objects(selected_device.device_address)
    _logger.info("\nAvailable Objects:")
    for idx, obj_id in enumerate(object_list):
        _logger.info(f"{idx + 1}. Object ID: {obj_id}")

    while True:
        try:
            obj_index = int(await async_input("Select object by index: ")) - 1
            if 0 <= obj_index < len(object_list):
                selected_object_id = list(object_list)[obj_index]
                break
            else:
                _logger.error("Invalid object index. Please enter a valid number.")
        except ValueError:
            _logger.error("Invalid input. Please enter a number.")

    # Display properties for user selection
    property_list = await app.check_writable_properties(selected_device.device_address, *selected_object_id)
    _logger.info("\nAvailable Properties:")
    for idx, prop_id in enumerate(property_list):
        _logger.info(f"{idx + 1}. Property ID: {prop_id}")

    while True:
        try:
            prop_index = int(await async_input("Select property by index: ")) - 1
            if 0 <= prop_index < len(property_list):
                selected_prop_id = property_list[prop_index]
                break
            else:
                _logger.error("Invalid property index. Please enter a valid number.")
        except ValueError:
            _logger.error("Invalid input. Please enter a number.")

    # Get confirmation from the user
    while True:
        confirmed_input = await async_input("Confirmed notifications? (yes/no): ").lower()
        if confirmed_input in ['yes', 'no']:
            confirmed = confirmed_input == 'yes'
            break
        else:
            _logger.error("Invalid input. Please enter 'yes' or 'no'.")

    while True:
        try:
            lifetime_seconds = int(await async_input("Enter lifetime in seconds (0 for indefinite): "))
            if lifetime_seconds >= 0:  # Ensure non-negative lifetime
                break
            else:
                _logger.error("Invalid input. Please enter a non-negative number.")
        except ValueError:
            _logger.error("Invalid input. Please enter a number.")

    # Create the subscription
    subscription = Subscription(selected_device.device_id, selected_object_id, selected_prop_id, confirmed, lifetime_seconds)
    await app.subscribe_cov(subscription)
        
        
async def handle_unsubscribe(app):
    """Handles the process of unsubscribing from a subscription."""
    _logger.info("\nActive Subscriptions:")
    for idx, (key, sub) in enumerate(app.subscriptions.items()):
        device_id, obj_id, prop_id = key
        device = app.discovered_devices.get(device_id)
        if device:
            _logger.info(
                f"{idx + 1}. Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}'"
            )

    while True:
        try:
            sub_index = int(await async_input("Select subscription to unsubscribe by index: ")) - 1
            if 0 <= sub_index < len(app.subscriptions):
                selected_subscription = list(app.subscriptions.values())[sub_index]

                # Display details of the selected subscription before confirmation
                device = app.discovered_devices.get(selected_subscription.device_id)
                _logger.info(
                    f"\nSelected Subscription:\n"
                    f"  Device: {device.device_name} ({selected_subscription.device_id})\n"
                    f"  Object: {selected_subscription.obj_id}\n"
                    f"  Property: {selected_subscription.prop_id}"
                )

                confirm = await async_input(f"Are you sure you want to cancel this subscription? (yes/no): ")
                if confirm.lower() == "yes":
                    await app.handle_unsubscribe(selected_subscription)
                    break
                else:
                    _logger.info("Subscription cancellation aborted.")
                    return
            else:
                _logger.error("Invalid subscription index. Please enter a valid number.")
        except ValueError:
            _logger.error("Invalid input. Please enter a number.")


async def handle_acknowledge_alarm(app):
    """Handles the process of acknowledging an alarm."""
    # Display active alarms for user selection
    _logger.info("\nActive Alarms:")
    for idx, alarm_key in enumerate(app.active_alarms):
        device_id, obj_id, prop_id, alarm_type = alarm_key
        device = app.discovered_devices.get(device_id)
        if device:
            _logger.info(f"{idx + 1}. Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}': {alarm_type}")

    try:
        alarm_index = int(await async_input("Select alarm to acknowledge by index: ")) - 1
        selected_alarm = list(app.active_alarms)[alarm_index]
        app.alarm_manager.acknowledged_alarms.add(selected_alarm)

        # Implement your alarm acknowledgment logic here (e.g., update database, send notification)
        _logger.info(f"Alarm {selected_alarm} acknowledged.")
    except (IndexError, ValueError):
        _logger.error("Invalid alarm selection.")
        
        
# ******************************************************************************


async def cli_loop(app):
    """Interactive command-line interface loop."""
    while True:
        print("\nBACee Menu:")
        print("1. Discover Devices")
        print("2. List Discovered Devices")
        print("3. Subscribe to Object Property")
        print("4. Unsubscribe from Object Property")
        print("5. View Active Alarms")
        print("6. Acknowledge Alarm")
        print("7. Quit")

        choice = await async_input("Enter your choice: ")

        if choice == '1':
            await app.discover_devices()
            # Display discovered devices in a user-friendly format
            for device in app.discovered_devices.values():
                _logger.info(f"Device ID: {device.device_id}, Name: {device.device_name}, Address: {device.device_address}")
            _logger.info("Discovery complete.")
        elif choice == '2':
            await handle_create_subscription(app)
        elif choice == '3':
            # Display subscriptions in a user-friendly format
            for key, sub in app.subscriptions.items():
                device_id, obj_id, prop_id = key
                device = app.discovered_devices.get(device_id)
                if device:
                    _logger.info(f"Subscription: Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}'")
        elif choice == '4':
            await handle_unsubscribe(app)
        elif choice == '5':
            # Display active alarms
            _logger.info("\nActive Alarms:")
            for alarm_key in app.active_alarms:
                device_id, obj_id, prop_id, alarm_type = alarm_key
                device = app.discovered_devices.get(device_id)
                if device:
                    _logger.info(f"Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}': {alarm_type}")
        elif choice == '6':
            await handle_acknowledge_alarm(app)
        elif choice == '7':
            break
        else:
            _logger.warning("Invalid choice")


# ******************************************************************************


# --- Flask ---

from flask_httpauth import HTTPBasicAuth

app_flask = Flask(__name__)
socketio = SocketIO(app_flask, cors_allowed_origins="*")  

# WebSocket Events

# Function to verify JWT token (replace with your actual implementation)
def verify_jwt(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return True
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return False
        
# Authentication decorator for SocketIO
def authenticated_only(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not request.sid or not session.get(request.sid):
            disconnect()
        else:
            return f(*args, **kwargs)
    return wrapped
    
@auth.verify_password
def verify_password(username, password):
   if username == 'admin' and password == 'password': # Replace with your credentials
       return True
   return False
   
SECRET_KEY = os.environ.get('SECRET_KEY', 'your_default_secret_key')  # Secret key for JWT signing
 
@socketio.on('connect')
def on_connect():
    _logger.info("WebSocket client connected")
    # Check for JWT token in the query string
    token = request.args.get('token')
    if not token or not verify_jwt(token):
        _logger.warning("Unauthorized WebSocket connection attempt")
        disconnect()  # Disconnect if invalid or missing token
    else:
        # Store user information in the session (if applicable)
        # ...
        session[request.sid] = True
        join_room(request.sid)

        _logger.info(f"User {request.sid} authenticated for WebSocket connection")
        emit('device_list', list(app.deviceInfoCache.get_device_identifiers()))  # Send initial device list

@socketio.on('subscribe_to_property')
@authenticated_only
async def on_subscribe(data):
    try:
        # ... (same as before)
    except (KeyError, ValueError) as e:
        _logger.error(f"Invalid subscription data: {e}")
        emit('subscription_error', {'error': str(e)})
    except CommunicationError as e:
        _logger.error(f"Communication error during subscription: {e}")
        emit('subscription_error', {'error': "Communication error with device"})
    except Exception as e:  # Catch any other unexpected errors
        _logger.error(f"Unexpected error during subscription: {e}")
        emit('subscription_error', {'error': "An unexpected error occurred"})

@socketio.on('unsubscribe_from_property')
@authenticated_only
async def on_unsubscribe(data):
    try:
        # ... (same as before)
    except (KeyError, ValueError) as e:
        _logger.error(f"Invalid unsubscribe data: {e}")
        emit('unsubscribe_error', {'error': str(e)})
    except Exception as e:
        _logger.error(f"Unexpected error during unsubscribe: {e}")
        emit('unsubscribe_error', {'error': "An unexpected error occurred"})

@socketio.on('authenticate')
def authenticate(auth):
    username = auth.get('username')
    password = auth.get('password')
    
    # Authenticate the user (Replace with your own authentication logic)
    if verify_password(username, password):  # Example using your existing function
        session[request.sid] = True # Save the sid in the session to verify for subsequent events
        join_room(request.sid)
        emit('authentication_success', {'message': 'Authentication successful'})
    else:
        disconnect()

@socketio.on('disconnect')
def on_disconnect():
    leave_room(request.sid)
    
# Flask API Endpoints

@app_flask.route('/devices')
@auth.login_required  # Require authentication for this endpoint
def get_devices():
    """API endpoint to get a list of discovered devices, with filtering and sorting."""
    filter_name = request.args.get("name")
    sort_by = request.args.get("sort_by", "device_id")
   
    devices = []
    for device_id in app.deviceInfoCache.get_device_identifiers():
        device_info = app.deviceInfoCache.get_device_info(device_id)
        if device_info and (not filter_name or filter_name.lower() in device_info.device_name.lower()):
            devices.append(
                {
                    "device_id": device_info.device_identifier[1],
                    "device_name": device_info.device_name,
                    "device_address": str(device_info.address),
                }
            )

    # Sorting
    if sort_by == "device_id":
        devices.sort(key=lambda x: x["device_id"])
    elif sort_by == "device_name":
        devices.sort(key=lambda x: x["device_name"])

    return jsonify(devices)

@app_flask.route("/devices/<int:device_id>/objects")
@auth.login_required  # Require authentication for this endpoint
def get_device_objects(device_id):
    """API endpoint to get a list of objects for a specific device."""
    objects = list(asyncio.run(app.iter_objects(device_id)))
    return jsonify(objects)


@app_flask.route('/objects/<object_type>/<int:object_instance>/properties')
@auth.login_required  # Require authentication for this endpoint
async def get_object_properties(object_type, object_instance):
    """API endpoint to get a list of properties and their values for a specific object."""
    obj_id = (object_type, object_instance)

    # Properties to read (including additional properties)
    properties_to_read = [
        "propertyList", "objectName", "description", "units", "presentValue", "statusFlags"
    ]  # Add more as needed

    # Device ID (you may need to change this to get from the request)
    device_id = int(await async_input(f"Enter the device ID for object {obj_id}: "))

    try:
        result = await app.property_reader.read_multiple_properties(
            device_id, obj_id, properties_to_read
        )
        if result is not None:
            property_values = {}
            for (obj_id, prop_id), value in result.items():
                if isinstance(value, ArrayOf):
                    value = value[0]
                # Add more type conversions or checks as needed
                property_values[prop_id] = value
            return jsonify(property_values)
        else:
            _logger.error(f"Error reading properties from device {device_id}: {result}")
            return jsonify({"error": "Failed to read properties"}), 500

    except (CommunicationError, TimeoutError) as e:
        _logger.error(f"Communication error with device {device_id}: {e}")
        return jsonify({"error": "Communication Error"}), 500


@app_flask.route('/objects/<object_type>/<int:object_instance>/properties/<property_name>', methods=['GET', 'PUT'])
@auth.login_required  # Require authentication for this endpoint
async def handle_property(object_type, object_instance, property_name):
    """API endpoint to read/write a property value with error handling."""
    obj_id = (object_type, object_instance)
    try:
        device_id = int(await async_input(f"Enter the device ID for object {obj_id}: "))
        device = app.discovered_devices.get(device_id)
        if device is None:
            return jsonify({"error": "Device not found"}), 404

        if request.method == 'GET':
            # Read property
            value = asyncio.run(app.property_reader.read_property(device_id, obj_id, property_name))
            if value is not None:
                return jsonify({"value": value[0]})  # Assuming the first element is the property value
            else:
                return jsonify({"error": "Failed to read property"}), 500

        if request.method == 'PUT':
            # Write property
            new_value = request.json.get('value')
            if new_value is None:
                return jsonify({"error": "Missing 'value' in request body"}), 400

            # Validate data type and range (optional)
            # ...

            asyncio.run(app.property_writer.write_property(device_id, obj_id, property_name, new_value))
            return jsonify({"message": "Property written successfully"})
    except (ValueError, KeyError) as e:
        return jsonify({"error": str(e)}), 400
    except BACpypesError as e:
        return jsonify({"error": "BACnet communication error"}), 500

@app_flask.route('/subscriptions', methods=['GET', 'POST', 'DELETE'])
@auth.login_required
async def handle_subscriptions():
    """API endpoint to manage COV subscriptions."""
    if request.method == 'GET':
        # Get all subscriptions
        subscriptions_data = []
        for sub, scm in app.subscriptions.items():
            subscriptions_data.append({
                'device_id': sub.device_id,
                'object_id': sub.obj_id,
                'property_id': sub.prop_id,
                'confirmed_notifications': sub.confirmed_notifications,
                'lifetime_seconds': sub.lifetime_seconds
            })
        return jsonify(subscriptions_data)

    elif request.method == 'POST':
        # Create a new subscription
        try:
            device_id = int(request.json.get('device_id'))
            object_type = request.json.get('object_type')
            object_instance = int(request.json.get('object_instance'))
            property_identifier = request.json.get('property_identifier')
            # confirmed_notifications = request.json.get('confirmed_notifications', True)  # Default to True
            # lifetime_seconds = request.json.get('lifetime_seconds')

            if not all([device_id, object_type, object_instance, property_identifier]):
                return jsonify({"error": "Missing required parameters."}), 400

            obj_id = (object_type, object_instance)

            device_info = app.deviceInfoCache.get_device_info(device_id)
            if device_info is None:
                return jsonify({"error": f"Device with ID {device_id} not found."}), 404


            # Validate object and property
            try:
                app.bbmd.validate_object_and_property(obj_id, property_identifier)
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

            # Create subscription and store it
            subscription = Subscription(device_id, obj_id, property_identifier)
            await app.subscribe_cov(subscription)
            return jsonify({"message": "Subscription created successfully."}), 201

        except (ValueError, KeyError) as e:
            return jsonify({"error": str(e)}), 400

    elif request.method == 'DELETE':
        try:
            device_id = int(request.json.get('device_id'))
            object_type = request.json.get('object_type')
            object_instance = int(request.json.get('object_instance'))
            property_identifier = request.json.get('property_identifier')
            
            obj_id = (object_type, object_instance)

            await app.unsubscribe_cov(device_id, obj_id, property_identifier)
            return jsonify({"message": "Subscription deleted successfully."}), 200
        except (ValueError, KeyError) as e:
            return jsonify({"error": str(e)}), 400


@app_flask.route('/alarms')
@auth.login_required
def get_alarms():
    alarms = app.alarm_manager.get_alarms()
    return jsonify(alarms)
    
@app_flask.route('/alarms', methods=['GET', 'PUT'])
@auth.login_required
async def handle_alarms():
    """API endpoint to view and acknowledge alarms."""
    if request.method == 'GET':
        # Get all active and acknowledged alarms
        active_alarms = list(app.active_alarms.values())
        acknowledged_alarms = list(app.acknowledged_alarms)
        all_alarms = active_alarms + acknowledged_alarms
        return jsonify(all_alarms)

    if request.method == 'PUT':
        # Acknowledge an alarm (similar to handle_acknowledge_alarm)
        try:
            alarm_index = int(request.json.get('alarm_index')) - 1
            if 0 <= alarm_index < len(app.active_alarms):
                selected_alarm = list(app.active_alarms.keys())[alarm_index]
                await app.alarm_manager.acknowledge_alarm(selected_alarm)
                return jsonify({"message": f"Alarm {selected_alarm} acknowledged."})
            else:
                return jsonify({"error": "Invalid alarm index."}), 400
        except (IndexError, ValueError, KeyError) as e:
            return jsonify({"error": str(e)}), 400

@app_flask.route('/alarms/acknowledge', methods=['POST'])
@auth.login_required
async def acknowledge_alarm():
    """API endpoint to acknowledge an alarm."""
    try:
        data = request.get_json()
        device_id = data.get('deviceId')
        object_id = data.get('objectId')
        property_id = data.get('propertyId')
        alarm_type = data.get('alarmType')

        if not all([device_id, object_id, property_id, alarm_type]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Create the alarm_key tuple
        alarm_key = (device_id, tuple(object_id), property_id, alarm_type)

        # Verify if the alarm is active
        if alarm_key not in app.alarm_manager.active_alarms:
            return jsonify({"error": "Alarm not found or already acknowledged"}), 404

        # Acknowledge the alarm
        await app.alarm_manager.acknowledge_alarm(alarm_key)

        return jsonify({"message": f"Alarm {alarm_type} on {object_id} of device {device_id} acknowledged successfully"}), 200
    
    except (ValueError, KeyError) as e:
        _logger.error(f"Error acknowledging alarm: {e}")
        return jsonify({"error": "Invalid request data"}), 400

@app_flask.route('/alarms/silence', methods=['POST'])
@auth.login_required
async def silence_alarm():
    """API endpoint to silence an alarm."""
    # Implement silence logic here

@app_flask.route('/alarms/history')
@auth.login_required
async def get_alarm_history():
    """API endpoint to get alarm history."""
    # Implement logic to fetch and return alarm history from the database

@app_flask.route('/alarms/silence', methods=['POST'])
@auth.login_required
async def silence_alarm():
    """API endpoint to silence an alarm."""
    try:
        device_id = int(request.json.get('device_id'))
        object_id = tuple(request.json.get('object_id'))
        property_id = request.json.get('property_id')
        alarm_type = request.json.get('alarm_type')
        duration = int(request.json.get('duration', 300))  # Default silence duration of 5 minutes (300 seconds)

        alarm_key = (device_id, object_id, property_id, alarm_type)
        app.alarm_manager.silence_alarm(device_id, obj_id, prop_id, alarm_type, duration)
        return jsonify({"message": "Alarm silenced successfully."}), 200
    except (ValueError, KeyError) as e:
        return jsonify({"error": str(e)}), 400

def start_api_server():
    app_flask.run(host="0.0.0.0", port=5000)  # Start on all interfaces, port 5000
    
# Configuration Validation Function (refactored)
def validate_configurations(configurations, validation_rules):
    """Validates configurations using a dictionary of rules."""
    issues_found = False
    for key, rule in validation_rules.items():
        value = configurations.get(key, "") 
        if not rule(value):
            logger.error(f"Invalid configuration: {key}={value}")
            issues_found = True
    return not issues_found  # Return True if valid

# JSON Validation Function (enhanced)
def validate_json_file(file_path, schema_file_path=None):
    """Validates JSON file and optionally against a schema."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        if schema_file_path:
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
            validate(instance=data, schema=schema)  # Schema validation
        return True
    except (json.JSONDecodeError, FileNotFoundError, jsonschema.ValidationError) as e:
        logger.error(f"JSON validation failed: {e}")
        return False

# Database Validation Function (example with SQLite)
def validate_database(db_path):
    """Validates database connection and schema."""
    try:
        with sqlite3.connect(db_path) as conn:
            # Execute schema validation queries here
            pass
        return True
    except sqlite3.Error as e:
        logger.error(f"Database validation failed: {e}")
        return False
        
# Main function
async def main():

    # Validations **************************************************************
    
    # Load configurations from environment variables or a file
    configurations = {
        "BBMD_ADDRESS": os.getenv("BBMD_ADDRESS"),
        "DEVICE_ID": os.getenv("DEVICE_ID"),
        # ... other configurations
    }

    # Define validation rules 
    validation_rules = {
        "BBMD_ADDRESS": lambda value: value and ":" in value,
        "DEVICE_ID": lambda value: value.isalnum(),
        # ... other rules
    }

    # Configuration Validation
    if not validate_configurations(configurations, validation_rules):
        logger.critical("Configuration validation failed. Exiting.")
        return  # Exit early if configurations are invalid

    # JSON and Database Validation
    json_file_path = "config.json"
    schema_file_path = "config_schema.json"
    if not validate_json_file(json_file_path, schema_file_path):
        logger.critical("JSON validation failed. Exiting.")
        return

    db_path = "mydb.db"
    if not validate_database(db_path):
        logger.critical("Database validation failed. Exiting.")
        return
        
    # **************************************************************************
        
    # BACnet Application Setup (async task handling)
    tasks = set()  # Set to keep track of running tasks
    try:
        logger.info("Starting BACnet application...")
        app = BACeeApp(LOCAL_ADDRESS, DEVICE_ID, DEVICE_NAME)

        async def start_and_register_bbmd():
            await app.start()
            if app.bbmd:
                await app.bbmd.register_foreign_device()

        # Create task for BACnet start and BBMD registration
        bacnet_task = asyncio.create_task(start_and_register_bbmd())
        tasks.add(bacnet_task)

        # Create tasks for subscription, alarm management, and CLI loop
        tasks.update(
            asyncio.create_task(coro)
            for coro in [app.manage_subscriptions(), app.manage_alarms(), cli_loop(app)]
        )

        # Start API server (typically runs continuously)
        api_server_task = asyncio.create_task(
            socketio.run(app_flask, host="0.0.0.0", port=5000)
        )
        tasks.add(api_server_task)  # Add to the set of tasks

        # Wait for any task to complete (normal or due to error)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Handle the finished task (optional, depending on your needs)
        for done_task in done:
            try:
                await done_task  # Ensure task is awaited
            except asyncio.CancelledError:
                pass
            except Exception as e:  # Catch exceptions that might occur in the task
                logger.exception("An error occurred in task:", exc_info=e)

        # Cancel any pending tasks
        for task in pending:
            task.cancel()
            try:
                await task  # Wait for the cancellation to complete
            except asyncio.CancelledError:
                pass

    except Exception as e:
        logger.exception("An error occurred in the main function:", exc_info=e)


if __name__ == '__main__':
    asyncio.run(main())
