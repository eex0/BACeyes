# Overall, this code serves as a robust foundation for building BACnet applications that require real-time monitoring 
# and control of devices across potentially complex network topologies.

# It can be customized and expanded to suit the specific requirements of various BACnet-based systems.

# This is a work in progress, there ARE bugs here!

import asyncio
import json
import logging
import os
import time
import sqlite3
import sys

import bacpypes3
from bacpypes3.app import Application
from bacpypes3.apdu import ErrorRejectAbortNack, SimpleAckPDU, ConfirmedCOVNotificationRequest, SimpleAckPDU, ReadPropertyRequest, WhoIsRequest, IAmRequest, SubscribeCOVRequest, UnconfirmedCOVNotificationRequest
from bacpypes3.app import Application
from bacpypes3.service.cov import Subscription
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.comm import bind
from bacpypes3.constructeddata import ArrayOf, SequenceOf
from bacpypes3.debugging import ModuleLogger
from bacpypes3.ipv4.app import NormalApplication
from bacpypes3.pdu import Address, LocalBroadcast
from bacpypes3.primitivedata import Unsigned
from bacpypes3.service.object import ReadWritePropertyServices

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, disconnect, join_room, leave_room
from flask_httpauth import HTTPBasicAuth
from jsonschema import validate
import jsonschema
from functools import wraps
import jwt
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
import aiosmtplib
from datetime import datetime

# ******************************************************************************


# Logging Configuration (with Console and File Logging)
_debug = 0
# Logging Configuration (with Console and File Logging)
_log = ModuleLogger(globals())  # Initialize the module logger
logging.basicConfig(
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

# Define custom exceptions for network communication and timeouts
class CommunicationError(Exception):
    pass

class TimeoutError(Exception):
    pass

                    

            
# ******************************************************************************

    
class BBMD:
    # __init__
    def __init__(self, address, db_file, topology_file="network_topology.json", bbmd_name=None):
        self.address = Address(address)
        self.db_file = db_file
        self.routing_table = {}
        self.topology_file = topology_file
        self.default_bbd_address = None
        self.app = None
        self.is_available = True  
        self.topology_data = None
        self.bbmd_name = bbmd_name
        self.load_configuration()
        self.load_topology()
        self.topology_watcher = asyncio.create_task(self.watch_topology_file())

    # load_configuration
def load_configuration(self):
    """Loads BBMD configuration from JSON file and database."""
    _log.debug("Loading BBMD configuration...")

    try:
        with open(self.topology_file, "r") as f:
            topology_data = json.load(f)
            bbmd_config = next(
                (
                    bbmd
                    for bbmd in topology_data.get("BBMDs", [])
                    if bbmd.get("name") == self.bbmd_name
                ),
                {},
            )
            self.broadcast_address = bbmd_config.get(
                "broadcastAddress", "255.255.255.255"
            )

        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT broadcast_address FROM bbmd_settings WHERE address = ?",
                (str(self.address),),
            )
            row = cursor.fetchone()
            if row:
                self.broadcast_address = row[0]
                _log.info(
                    f"BBMD configuration loaded from database: {self.address} - broadcastAddress: {self.broadcast_address}"
                )
            else:
                _log.info(
                    f"BBMD configuration loaded from JSON: {self.address} - broadcastAddress: {self.broadcast_address}"
                )

    except FileNotFoundError as e:
        _log.warning(f"Topology file {self.topology_file} not found: {e}")
    except json.JSONDecodeError as e:
        _log.error(f"Error decoding topology file {self.topology_file}: {e}")
    except sqlite3.Error as e:
        _log.error(f"Database error while loading configuration: {e}")
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Unexpected error loading BBMD configuration: {e}")
        self.broadcast_address = "255.255.255.255"  # Default to broadcast in case of error


    # update_configuration
    def update_configuration(self, new_broadcast_address):
        """Updates the BBMD configuration in both JSON file and database."""
        _log.debug(
            f"Updating BBMD configuration for {self.address} with broadcastAddress: {new_broadcast_address}"
        )
        try:
            # Update JSON file
            with open(self.topology_file, "r+") as f:
                topology_data = json.load(f)
                for bbmd in topology_data.get("BBMDs", []):
                    if bbmd.get("name") == self.bbmd_name:
                        bbmd["broadcastAddress"] = new_broadcast_address
                        break
                f.seek(0)
                json.dump(topology_data, f, indent=4)
                f.truncate()

            # Update database
            with sqlite3.connect(self.db_file) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE bbmd_settings SET broadcast_address = ? WHERE address = ?",
                    (new_broadcast_address, str(self.address)),  # Use str(self.address) for database compatibility
                )

            self.broadcast_address = new_broadcast_address
            _log.info(
                f"BBMD configuration updated: {self.address} - broadcastAddress: {new_broadcast_address}"
            )

        except FileNotFoundError as e:
            _log.error(f"Topology file not found while updating configuration: {e}")
        except json.JSONDecodeError as e:
            _log.error(f"Error decoding topology file while updating configuration: {e}")
        except sqlite3.Error as e:
            _log.error(f"Database error while updating configuration: {e}")
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error updating BBMD configuration: {e}")

    # discover_bbmds
    async def discover_bbmds(self):
        """Discovers available BBMDs on the network."""
        _log.debug("Initiating BBMD discovery process...")
    
        try:
            who_is = WhoIsRequest()
            who_is.pduDestination = Address(self.broadcast_address)  # Use configured broadcast address
            self.app.request(who_is)  # Broadcast WhoIs
    
            await asyncio.sleep(5)  # Adjust wait time if necessary
    
            discovered_bbmds = []
            for device in self.app.deviceInfoCache.get_device_infos():
                if device.isBBMD:
                    _log.info(f"Discovered BBMD: {device.address}")
                    discovered_bbmds.append(device)
            return discovered_bbmds
    
        except (CommunicationError, TimeoutError) as e:
            _log.error(f"Error discovering BBMDs: {e}")
            return []  # Return an empty list to indicate no BBMDs found
    
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error during BBMD discovery: {e}")
            return []
        
    async def select_bbmd(self, bbmds):
        """Selects a BBMD for routing using a round-robin strategy."""
        _log.debug(f"Selecting BBMD from {len(bbmds)} discovered BBMDs...")
    
        if not bbmds:
            _log.warning("No BBMDs available for selection.")
            return None
        
        # Round-robin selection
        next_index = self.app.bbmd_round_robin_index
        self.app.bbmd_round_robin_index = (next_index + 1) % len(bbmds)
        selected_bbmd = bbmds[next_index]
    
        _log.info(f"Selected BBMD: {selected_bbmd.address}")
        return selected_bbmd


    # load_topology
def load_topology(self):
    """Loads network topology and BBMDs from JSON file."""
    retries = 2
    while retries >= 0:
        try:
            with open(self.topology_file, "r") as f:
                self.topology_data = json.load(f)
                _log.info(f"Network topology loaded from {self.topology_file}")  
                break  # exit the loop if the file is read successfully.
        except FileNotFoundError as e:
            retries -= 1
            if retries >= 0:  # if there are retries left
                _log.warning(f"Topology file {self.topology_file} not found: {e}. Retrying in 3 seconds...")
                time.sleep(3)  # Wait 3 seconds before retrying
            else:
                _log.error(f"Topology file {self.topology_file} not found after multiple retries. Using default values.")
                self.topology_data = {}  # Use an empty dictionary as default
                break  # Exit the loop after all retries failed
        except json.decoder.JSONDecodeError as e:
            _log.error(f"Error decoding topology file {self.topology_file}: {e}. Using default values.")
            self.topology_data = {}
            break  # Exit the loop, decoding will not work on subsequent retries.
        except KeyError as e:
            _log.error(f"Missing key in topology file {self.topology_file}: {e}. Using default values.")
            self.topology_data = {}
            break  # Exit the loop, as the file format is wrong
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error loading topology: {e}")
            self.topology_data = {}
            break  # Exit the loop on any other exception
    

    bbmd_addresses = [
        Address(entry.get("address"))
        for entry in self.topology_data.get("BBMDs", [])
        if isinstance(entry.get("address"), str)
    ] 
    self.bbmds = bbmd_addresses  # Store BACpypes3 Address objects

    # Load default BBD address
    default_bbd_config = next(
        (
            bbmd
            for bbmd in self.topology_data.get("BBMDs", [])
            if bbmd.get("isDefaultBBD", False)
        ),
        {},
    )
    self.default_bbd_address = default_bbd_config.get("address")

            
    # watch_topology_file
async def watch_topology_file(self):
    """Asynchronously monitors the topology file for changes and reloads it."""
    last_modified = 0
    retries = 0
    max_retries = 3
    while True:
        try:
            current_modified = os.path.getmtime(self.topology_file)
            if current_modified > last_modified:
                _log.info("Topology file changed, reloading...")
                self.load_topology()  # Reload topology
                last_modified = current_modified

                # Trigger actions for topology changes (e.g., updating subscriptions)
                await self.handle_topology_change()  

            retries = 0 # Reset retries on success
            await self.check_and_subscribe_new_devices()

        except FileNotFoundError as e:
            if retries < max_retries:
                retries += 1
                _log.warning(f"Topology file not found. Attempt {retries}/{max_retries}. Retrying in 3 seconds...")
                await asyncio.sleep(3)  # Retry after 3 seconds
            else:
                _log.error(f"Topology file not found after {max_retries} retries. Stopping topology watcher.")
                return
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error while watching topology file: {e}")
            # You might want to add a delay here before the next loop iteration
        
        await asyncio.sleep(5)  # Check every 5 seconds


    # request_routing_table
async def request_routing_table(self):
    """Requests the routing table from the BBMD."""
    _log.debug(f"Requesting routing table from BBMD at {self.address}")
    
    try:
        request = ReadPropertyRequest(
            objectIdentifier=('device', self.device_id),
            propertyIdentifier='routingTable'
        )
        request.pduDestination = self.address

        response = await self.app.request(request)

        if isinstance(response, ReadPropertyACK):
            try:
                routing_table = response.propertyValue.cast_out(SequenceOf(SequenceOf(Unsigned)))
                _log.debug(f"Received routing table: {routing_table}")
                return routing_table
            except Exception as e:  
                _log.error(f"Error extracting routing table data: {e}")
        elif isinstance(response, (Error, Reject, Abort)):
            _log.error(f"Error in routing table response: {response.errorClass} - {response.errorCode}")
        else:
            _log.error(f"Unexpected response when requesting routing table: {response}")

    except TimeoutError as e:
        _log.error(f"Timeout error while requesting routing table: {e}")
    except BACpypesError as e:
        _log.error(f"BACpypes error requesting routing table: {e}")

    return None  # Indicate failure

    
    
    # is_available
    def is_available(self):
        """
        Checks if the BBMD is available (registered and has a recent routing table).
        """
        try:
            last_update_time = self.last_update_time
        except AttributeError:
            last_update_time = 0
            
        return self.registered and self.routing_table is not None and \
               (time.time() - last_update_time) < 60
    
    
    # register_foreign_device
    async def register_foreign_device(self, ttl=600):
        """Registers the local device as a foreign device with the BBMD."""
        _log.debug(f"Registering as a foreign device with BBMD at {self.address}")
    
        if not self.app or not hasattr(self.app, 'localDevice') or not hasattr(self.app.localDevice, 'objectIdentifier'):
            _log.error("Local device information not available for registration.")
            return
        
        request = WritePropertyRequest(
            objectIdentifier=("device", self.app.localDevice.objectIdentifier[1]), # Assumes local device object exists
            propertyIdentifier="registerForeignDevice",
            propertyValue=ArrayOf(Unsigned)([self.app.localDevice.objectIdentifier[1], ttl]),  # use the device id from localDevice
        )
        request.pduDestination = self.address
    
        try:
            response = await self.app.request(request)
            if isinstance(response, SimpleAckPDU):
                _log.info(f"Registered as a foreign device with BBMD: {response}")
                self.registered = True
                self.last_update_time = time.time()
                self.routing_table = await self.request_routing_table()
            else:
                _log.error(f"Failed to register as foreign device: {response}")
        except (CommunicationError, TimeoutError) as e:
            _log.error(f"Error registering as a foreign device: {e}")
    
    
    # get_destination_address
    def get_destination_address(self, device_id):
        """Gets the destination address for a device, considering BBMD routing."""
    
        _log.debug(f"Determining destination address for device {device_id}")
    
        # Check if the device is local
        if device_id[0] == self.app.localDevice.objectIdentifier[0]:
            _log.debug(f"Device {device_id} is on the local network.")
            return Address(device_id)  # Device is local, no routing needed
    
        # Iterate over available BBMDs
        for bbmd in self.app.bbmd_manager.bbmds.values():
            if bbmd.is_available():
                _log.debug(f"Checking BBMD {bbmd.address} for route...")
                for route in bbmd.routing_table:
                    if isinstance(route, SequenceOf) and len(route) >= 2 and route[1].pduSource == Address(device_id):
                        _log.debug(f"Found route to {device_id} through BBMD {bbmd.address}")
                        return bbmd.address  # Route through the BBMD
    
        _log.warning(f"No route found for device {device_id}. Using direct addressing.")
        return Address(device_id)  # No suitable BBMD, use direct addressing
    
    # update_routing_table
    async def update_routing_table(self):
        """Requests and updates the BBMD's routing table."""
        _log.info("Updating routing table from BBMD")
        new_routing_table = await self.request_routing_table()
        if new_routing_table is not None:
            self.routing_table = new_routing_table
            self.last_update_time = time.time()
            
async def validate_object_and_property(self, device_id, obj_id, prop_id):
    """Validates if the object and property exist on the device using BACpypes3 services."""
    _log.debug(f"Validating object {obj_id} and property {prop_id} on device {device_id}")

    try:
        # 1. Read Object List
        object_list = await self.get_object_list(device_id)
        if obj_id not in object_list:
            _log.warning(f"Object {obj_id} not found on device {device_id}")
            return False

        # 2. Read Property List for the Object
        property_list_result = await self.property_reader.read_property(device_id, obj_id, 'propertyList')
        if not isinstance(property_list_result, ReadPropertyACK):
            _log.error(f"Failed to read propertyList for object {obj_id} on device {device_id}")
            return False

        property_list = [prop.identifier for prop in property_list_result.propertyValue[0].value]

        # 3. Check if Property is in the List
        if prop_id not in property_list:
            _log.warning(f"Property {prop_id} not found for object {obj_id} on device {device_id}")
            return False

        return True  # Object and property are valid

    except (CommunicationError, TimeoutError, BACpypesError) as e:
        _log.error(f"Error validating object and property on device {device_id}: {e}")
        return False
            
    # handle_topology_change (New method)
    async def handle_topology_change(self):
        """
        Reacts to changes in the topology file.
        """
        _log.info("Handling topology change...")
    
        # 1. Update list of BBMDs
        bbmd_addresses = [
            Address(entry.get("address"))
            for entry in self.topology_data.get("BBMDs", [])
            if isinstance(entry.get("address"), str)
        ]  # Filter out invalid addresses
        self.bbmds = bbmd_addresses
    
        # 2. Update default BBD address
        default_bbd_config = next(
            (
                bbmd
                for bbmd in self.topology_data.get("BBMDs", [])
                if bbmd.get("isDefaultBBD", False)
            ),
            {},
        )
        self.default_bbd_address = default_bbd_config.get("address")
    
        # 3. If this BBMD is the default, trigger network scan
        if self.default_bbd_address == self.address:
            asyncio.create_task(self.app.discover_devices())  # Trigger device discovery
    
        # 4. Update or create subscriptions based on the new topology (see previous response for example)
    
    # check_and_subscribe_new_devices (New method)
    async def check_and_subscribe_new_devices(self):
        """
        Checks for new devices in the topology and subscribes to their properties.
        """
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
                    _log.info(f"Subscribed to {obj_id}.{property_identifier} on device {device_id}")
                except ValueError as e:
                    _log.error(f"Invalid subscription: {e}")
    
               
# ******************************************************************************

# obsolete

class BBMDManager:
    def __init__(self, topology_file="network_topology.json"):
        self.topology_file = topology_file
        self.bbmds = {}  # Dictionary to store BBMD objects

    def load_bbmd_configurations(self):
        try:
            with open(self.topology_file, "r") as f:
                topology_data = json.load(f)
                for bbmd_config in topology_data.get("BBMDs", []):
                    address = bbmd_config["address"]
                    bbmd = BBMD(address)
                    bbmd.broadcast_address = bbmd_config.get("broadcastAddress", "255.255.255.255")
                    self.bbmds[address] = bbmd
        except (FileNotFoundError, json.JSONDecodeError) as e:
            _log.error(f"Error loading topology file: {e}")

    def get_bbmd(self, address):
        return self.bbmds.get(address)

    # ... (methods to add, remove, update BBMDs) ...
    
    
# ******************************************************************************


class AlarmManager:
    def __init__(self, app: Application):  # Now BIPSimpleApplication is defined
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
    _log.debug(f"Handling COV notification for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}: Value={property_value}")

    obj_id = subscription.obj_id
    prop_id = subscription.prop_id
    device_id = subscription.device_id

    try:
        # 1. Get Recent Values for Trend Analysis
        history = self.app.cov_history.get(obj_id, {}).get(prop_id, [])
        recent_values = history[-10:]  # Get the last 10 values or fewer

        # 2. Change Filtering
        if subscription.change_filter and recent_values:
            previous_value = recent_values[-1][1]
            if abs(property_value - previous_value) < subscription.change_filter:
                _log.debug(f"Skipping notification for {obj_id}.{prop_id} due to change filter.")
                return

        # 3. Alarm Flood Detection
        alarm_key = (device_id, obj_id, prop_id)
        if not self.is_alarm_silenced(alarm_key):
            await self.detect_alarm_flood(alarm_key)

        # 4. Alarm Logic (Only if not in alarm flood)
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

        # 5. Anomaly Detection (Using Z-Score)
        if len(recent_values) >= 2:  # Ensure enough data for Z-score calculation
            _, values = zip(*recent_values)
            z_scores = (np.array(values) - np.mean(values)) / np.std(values)
            if any(abs(z) > 2 for z in z_scores):  # Threshold of 2 for anomaly
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

    except BACpypesError as e:
        _log.error(f"BACpypes error handling COV notification: {e}")


async def trigger_alarm(self, device_id, obj_id, prop_id, alarm_type, alarm_value, priority=None, z_score=None, severity="medium", history=None):
    """Triggers an alarm, stores it, and sends a notification."""
    _log.debug(f"Entering trigger_alarm for {obj_id}.{prop_id} on {device_id} with type {alarm_type} and value {alarm_value}")

    alarm_key = (device_id, obj_id, prop_id, alarm_type)

    # Check for existing alarm and update if necessary (same as before)
    existing_alarm = self.active_alarms.get(alarm_key)
    if existing_alarm:
        if existing_alarm["severity"] != severity or existing_alarm["alarm_value"] != alarm_value:
            _log.info(f"Updating active alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}: Value={alarm_value}, Z-score={z_score}, Severity={severity}")
            existing_alarm.update({
                "timestamp": time.time(),
                "alarm_value": alarm_value,
                "z_score": z_score,
                "severity": severity,
            })

            try:
                self.app.save_alarm_to_db(*alarm_key, alarm_value, z_score, existing_alarm["is_anomaly"])
            except sqlite3.IntegrityError as e:
                _log.error(f"Database integrity error while updating alarm: {e}")
            except sqlite3.Error as e:
                _log.error(f"Database error while updating alarm: {e}")
        else:
            _log.debug(f"Alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id} already active and unchanged.")
            return  # No need to trigger again
    else:  # New alarm
        _log.info(f"Triggering new alarm '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}: Value={alarm_value}, Z-score={z_score}")

        # Initialize alarm data
        self.active_alarms[alarm_key] = {
            "timestamp": time.time(),
            "alarm_type": alarm_type,
            "alarm_value": alarm_value,
            "priority": priority,
            "z_score": z_score,
            "severity": severity,
            "is_anomaly": alarm_type == "Anomaly",
        }

        try:
            self.app.save_alarm_to_db(*alarm_key, alarm_value, z_score, self.active_alarms[alarm_key]["is_anomaly"])
        except sqlite3.IntegrityError as e:
            _log.error(f"Database integrity error while triggering alarm: {e}")
        except sqlite3.Error as e:
            _log.error(f"Database error while triggering alarm: {e}")

    # Send initial notification (level 1) (same as before)
    asyncio.create_task(self.send_alarm_notification(alarm_key, history=history))

    # Escalation for new or upgraded critical alarms (same as before)
    if severity == "critical" and (existing_alarm is None or existing_alarm["severity"] != "critical"):
        asyncio.create_task(self.escalate_alarm(alarm_key))
    
    
    async def escalate_alarm(self, alarm_key):
        """Escalates an alarm if not acknowledged within the specified timeframe."""
        _log.debug(f"Starting escalation for alarm {alarm_key}")
    
        try:
            await asyncio.sleep(900)  # Wait for 15 minutes
    
            if alarm_key in self.active_alarms and alarm_key not in self.acknowledged_alarms:
                _log.warning(f"Escalating alarm {alarm_key} to level 2")
                await self.send_alarm_notification(alarm_key, escalation_level=2)
            else:
                _log.info(f"Alarm {alarm_key} already acknowledged or cleared. No escalation needed.")
        
        except asyncio.CancelledError:
            _log.warning(f"Escalation task for alarm {alarm_key} cancelled.")
    
    
    async def send_alarm_notification(self, alarm_key, escalation_level=1, history=None):
        """Sends an alarm notification with escalation level and details."""
        _log.debug(f"Sending alarm notification for {alarm_key} at escalation level {escalation_level}")
    
        device_id, obj_id, prop_id, alarm_type = alarm_key
        alarm_data = self.active_alarms.get(alarm_key)
    
        if alarm_data is None:
            _log.error(f"Alarm data for {alarm_key} not found.")
            return
    
        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if device_info is None:
            _log.error(f"Device with ID {device_id} not found. Cannot send alarm notification.")
            return
    
        recipients = self._get_notification_recipients(escalation_level)
        message_content = self._format_alarm_message(alarm_data, device_info, history)
    
        # Use asyncio.gather for concurrent email sending, but handle potential errors
        tasks = [self.app.send_email_notification(message_content, recipient) for recipient in recipients]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
        for recipient, result in zip(recipients, results):
            if isinstance(result, Exception):
                _log.error(f"Failed to send alarm notification email to {recipient}: {result}")
            else:
                _log.info(f"Alarm notification email sent to {recipient}")
    
    
    def _get_notification_recipients(self, escalation_level):
        """Determines notification recipients based on the escalation level."""
        _log.debug(f"Getting recipients for escalation level {escalation_level}")
        
        # Fetch recipients from database based on escalation level and alarm severity
        try:
            with self.app.db_conn:
                cursor = self.app.db_conn.cursor()
                cursor.execute(
                    "SELECT email FROM alarm_recipients WHERE escalation_level <= ? ORDER BY escalation_level ASC", 
                    (escalation_level,)
                )
                recipients = [row[0] for row in cursor.fetchall()]
    
                if not recipients:  # Default to admin if no recipients found
                    _log.warning(f"No recipients found for escalation level {escalation_level}. Using default admin.")
                    return ["admin@example.com"]
    
                _log.debug(f"Found recipients: {recipients}")
                return recipients
    
        except sqlite3.Error as e:
            _log.error(f"Error retrieving recipients from database: {e}")
            return ["admin@example.com"]  # Fallback to admin in case of error
    
    def _format_alarm_message(self, alarm_data, device_info, history=None):
        """Formats the alarm message content."""
        _log.debug(f"Formatting alarm message for {alarm_data['alarm_type']} alarm on device {device_info.device_name}")
        
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
            
            # Add Z-score if it's an anomaly alarm
            if alarm_data.get('is_anomaly') and 'z_score' in alarm_data:
                message_content += f"\nZ-score: {alarm_data['z_score']}"
    
            if history and alarm_data.get('is_anomaly'):
                trend_info = self._calculate_trend(history)
                message_content += f"\nRecent Trend: {trend_info}"
        except KeyError as e:
            _log.error(f"Error formatting alarm message: Missing key in alarm_data - {e}")
            message_content = "Error: Invalid alarm data"
    
        return message_content
    
    def _calculate_trend(self, history, threshold=0.05):  # Add a threshold parameter
        """Calculates a trend from the given history based on a percentage change threshold."""
        _log.debug(f"Calculating trend from history: {history}")
    
        if len(history) < 2:
            return "Insufficient data"
        
        _, values = zip(*history)  # Extract only the values from history
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
     
async def clear_alarm(self, device_id, obj_id, prop_id, alarm_type):
    """Clears a previously triggered alarm."""
    _log.debug(f"Clearing alarm of type '{alarm_type}' for {obj_id}.{prop_id} on device {device_id}")

    alarm_key = (device_id, obj_id, prop_id, alarm_type)

    # Remove from acknowledged alarms (if present)
    if alarm_key in self.acknowledged_alarms:
        self.acknowledged_alarms.remove(alarm_key)

    try:
        # Check for alarm in active alarms
        if alarm_key in self.active_alarms:
            # Remove from active alarms
            del self.active_alarms[alarm_key]

            # Update the database to mark the alarm as cleared
            with self.app.db_conn:
                cursor = self.app.db_conn.cursor()
                cursor.execute(
                    "UPDATE alarms SET cleared = 1, timestamp = ? WHERE device_id = ? AND object_id = ? AND property_id = ? AND alarm_type = ?",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), device_id[1], str(obj_id), prop_id, alarm_type)
                )

            _log.info(f"Alarm {alarm_key} cleared in database.")

            # Send notification of alarm clearing
            await self.send_alarm_notification(alarm_key, message_prefix="CLR: ")

            # Cancel escalation task if one is running
            self._cancel_escalation_task(alarm_key)
        else:
            _log.warning(f"Tried to clear an alarm that was not active: {alarm_key}")
    except sqlite3.IntegrityError as e:
        _log.error(f"Database integrity error while clearing alarm: {e}")
    except sqlite3.Error as e:
        _log.error(f"Error clearing alarm in database: {e}")

                              
        # In the trigger_alarm function
        # await self.send_alarm_notification(alarm_key)  # Initial notification (level 1)
    
        # In the escalate_alarm function
        # await self.send_alarm_notification(alarm_key, escalation_level=2)  # Escalation (level 2)
    
    async def send_email_notification(self, message_content, recipient_email):
        """Sends an email notification using SMTP (e.g., Gmail)."""
        _log.debug(f"Sending email notification to {recipient_email}")
    
        sender_email = os.environ.get("EMAIL_SENDER")
        password = os.environ.get("EMAIL_PASSWORD")
    
        if not sender_email or not password:
            _log.error("Email credentials not found in environment variables. Cannot send notification.")
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
                _log.info(f"Email notification successfully sent to {recipient_email}")
    
        except smtplib.SMTPException as e:
            _log.error(f"SMTP error sending notification to {recipient_email}: {e}")
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error sending notification to {recipient_email}: {e}")
    
       
    async def manage_alarms(self):
        """Periodically checks active alarms and sends reminders if they persist."""
        _log.debug("Starting alarm management task")
    
        while True:
            try:
                current_time = time.time()
                for alarm_key, alarm_data in list(self.active_alarms.items()):
                    if current_time - alarm_data['timestamp'] > self.reminder_interval:
                        _log.info(f"Sending reminder for persistent alarm {alarm_key}")
                        await self.send_reminder_notification(alarm_key)
    
                # Check for silenced alarms that have expired
                self.remove_expired_silenced_alarms()
    
                await asyncio.sleep(self.reminder_interval)
            
            except Exception as e:
                _log.error(f"Unexpected error in manage_alarms: {e}")
                await asyncio.sleep(self.reminder_interval)  # Wait before retrying
    
        
    async def send_reminder_notification(self, alarm_key):
        """Sends a reminder notification for the given alarm."""
        _log.debug(f"Sending reminder notification for alarm {alarm_key}")
    
        device_id, obj_id, prop_id, alarm_type = alarm_key
        alarm_data = self.active_alarms.get(alarm_key)
    
        if alarm_data is None:
            _log.error(f"Alarm data for {alarm_key} not found.")
            return
    
        device_info = self.app.deviceInfoCache.get_device_info(device_id)
        if device_info is None:
            _log.error(
                f"Device with ID {device_id} not found. Cannot send reminder notification."
            )
            return
    
        message_content = f"""
        BACnet Alarm Reminder
    
        Severity: {alarm_data['severity']}
        Alarm Type: {alarm_data['alarm_type']}
        Device: {device_info.device_name} ({device_info.device_identifier[1]})
        Object: {alarm_data['object_id']}
        Property: {alarm_data['property_id']}
        Value: {alarm_data['alarm_value']}
        """
        
        # Send email (or other notification)
        recipients = self._get_notification_recipients(1)  # Get recipients for level 1
        tasks = [self.app.send_email_notification(message_content, recipient) for recipient in recipients]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
        for recipient, result in zip(recipients, results):
            if isinstance(result, Exception):
                _log.error(f"Failed to send reminder notification email to {recipient}: {result}")
            else:
                _log.info(f"Reminder notification email sent to {recipient}")
    
    async def acknowledge_alarm(self, alarm_key):
        """Acknowledges an alarm, moving it from active to acknowledged and updates the database."""
        _log.debug(f"Acknowledging alarm {alarm_key}")
    
        try:
            if alarm_key in self.active_alarms:
                # Move alarm to acknowledged set
                self.acknowledged_alarms.add(alarm_key)
    
                # Remove from active alarms
                del self.active_alarms[alarm_key]
    
                # Update the database to mark the alarm as acknowledged
                await self.update_alarm_acknowledgment_in_db(alarm_key, acknowledged=True)
    
                # Send notification of acknowledgment
                await self.send_alarm_notification(alarm_key, message_prefix="ACK: ")
    
                # Cancel any ongoing escalation tasks
                self._cancel_escalation_task(alarm_key)
            else:
                _log.warning(f"Alarm {alarm_key} not found in active alarms. Cannot acknowledge.")
        except Exception as e:
            _log.exception(f"An unexpected error occurred while acknowledging alarm {alarm_key}: {e}")
    
    
    async def update_alarm_acknowledgment_in_db(self, alarm_key, acknowledged):
        """Updates the acknowledgment status of an alarm in the database."""
        _log.debug(f"Updating alarm acknowledgment in database for {alarm_key}: acknowledged={acknowledged}")
        try:
            with self.app.db_conn:  # Use context manager for automatic transactions
                cursor = self.app.db_conn.cursor()
                device_id, obj_id, prop_id, alarm_type = alarm_key
                cursor.execute(
                    "UPDATE alarms SET acknowledged = ?, cleared = ? WHERE device_id = ? AND object_id = ? AND property_id = ? AND alarm_type = ?",
                    (acknowledged, not acknowledged, device_id[1], str(obj_id), prop_id, alarm_type)
                )  
            _log.debug(f"Alarm acknowledgment updated in database for {alarm_key}")
        except sqlite3.Error as e:
            _log.error(f"Error updating alarm acknowledgment in database: {e.args[0]}")
    
    
    def _cancel_escalation_task(self, alarm_key):
        """Cancels the escalation task for the specified alarm."""
        for task in asyncio.all_tasks():
            if task.get_name() == f"escalate_{alarm_key}":  
                task.cancel()
                _log.debug(f"Escalation task for {alarm_key} cancelled")
                break
        else:
            _log.debug(f"No escalation task found for {alarm_key}")
    
    
    # save_cov_notification_to_db
    def save_cov_notification_to_db(self, device_id, obj_id, prop_id, value):
        """Saves COV notification data to the database."""
        _log.debug(f"Saving COV notification to database: {obj_id}.{prop_id} = {value} (Device {device_id})")
        try:
            device_id = int(device_id[1])  # Extract device instance from tuple
            cursor = self.app.db_conn.cursor()
            cursor.execute(
                "INSERT INTO cov_notifications (timestamp, device_id, object_id, property_id, value) VALUES (?, ?, ?, ?, ?)",
                (time.time(), device_id, str(obj_id), prop_id, str(value)),  # Parameterized query
            )
            self.app.db_conn.commit()  
        except sqlite3.Error as e:
            _log.error(f"Error saving COV notification to database: {e}")
            self.app.db_conn.rollback()  # Rollback on error
    
    
    # save_alarm_to_db
    def save_alarm_to_db(self, device_id, obj_id, prop_id, alarm_type, alarm_value, z_score=None, is_anomaly=False):
        """Saves alarm data to the database."""
        _log.debug(f"Saving alarm to database: {alarm_type} for {obj_id}.{prop_id} on device {device_id}")
        try:
            device_id = int(device_id[1])
            z_score = float(z_score) if z_score is not None else None
            is_anomaly = int(is_anomaly)
    
            cursor = self.app.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO alarms 
                (timestamp, device_id, object_id, property_id, alarm_type, alarm_value, z_score, is_anomaly, acknowledged, cleared)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), device_id, str(obj_id), prop_id, alarm_type, str(alarm_value), z_score, is_anomaly, False, False)
            )
            self.app.db_conn.commit()
        except sqlite3.Error as e:
            _log.error(f"Error saving alarm to database: {e}")
            self.app.db_conn.rollback()
    
    
    # load_silenced_alarms
    def load_silenced_alarms(self):
        """Loads silenced alarms from the database into memory."""
        _log.debug("Loading silenced alarms from database")
    
        try:
            cursor = self.app.db_conn.cursor()
            cursor.execute("SELECT device_id, object_id, property_id, alarm_type, silence_end_time FROM silenced_alarms")
            rows = cursor.fetchall()
            for row in rows:
                device_id, object_id, property_id, alarm_type, silence_end_time = row
                alarm_key = (device_id, tuple(map(int, object_id[1:-1].split(", "))), property_id, alarm_type)
                self.silenced_alarms[alarm_key] = silence_end_time
            _log.info(f"Silenced Alarms Successfully Loaded: {self.silenced_alarms}")
        except sqlite3.Error as e:
            _log.error(f"Error loading silenced alarms from database: {e}")
    
    # is_alarm_silenced
    def is_alarm_silenced(self, alarm_key):
        """Checks if an alarm is currently silenced."""
        silence_end_time = self.silenced_alarms.get(alarm_key)
        return silence_end_time is not None and time.time() < silence_end_time
    
    
    def silence_alarm(self, device_id, obj_id, prop_id, alarm_type, duration):
        """Silences an alarm."""
        _log.debug(f"Silencing alarm of type '{alarm_type}' for {obj_id}.{prop_id} on device {device_id} for {duration} seconds.")
        
        alarm_key = (device_id, obj_id, prop_id, alarm_type)
        silence_end_time = time.time() + duration
        self.silenced_alarms[alarm_key] = silence_end_time
    
        try:
            with self.app.db_conn:
                cursor = self.app.db_conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO silenced_alarms (device_id, object_id, property_id, alarm_type, silence_end_time) VALUES (?, ?, ?, ?, ?)",
                    (device_id, str(obj_id), prop_id, alarm_type, silence_end_time),
                )
            _log.info(f"Alarm {alarm_key} silenced for {duration} seconds and stored in the database.")
        except sqlite3.Error as e:
            _log.error(f"Error silencing alarm in database: {e}")
    
    
    def remove_expired_silenced_alarms(self):
        """Removes silenced alarms that have expired."""
        _log.debug("Checking for expired silenced alarms")
    
        current_time = time.time()
        expired_alarms = [key for key, silence_end_time in self.silenced_alarms.items() if silence_end_time < current_time]
        
        for alarm_key in expired_alarms:
            del self.silenced_alarms[alarm_key]
            try:
                with self.app.db_conn:
                    cursor = self.app.db_conn.cursor()
                    cursor.execute(
                        "DELETE FROM silenced_alarms WHERE device_id = ? AND object_id = ? AND property_id = ? AND alarm_type = ?",
                        alarm_key
                    )
                _log.info(f"Removed expired silenced alarm {alarm_key} from database")
            except sqlite3.Error as e:
                _log.error(f"Error removing silenced alarm from database: {e}")
    
async def detect_alarm_flood(self, alarm_key):
    """Detects and handles alarm floods."""
    _log.debug(f"Checking for alarm flood: {alarm_key}")

    device_id = alarm_key[0]
    now = time.time()
    time_window_start = now - (now % self.flood_detection_window)

    try:
        self.alarm_counts[device_id][time_window_start] += 1
        if (
            self.alarm_counts[device_id][time_window_start] > self.flood_threshold
            and not self.alarm_flood_active[device_id]
        ):
            _log.warning(f"Alarm flood detected on device {device_id}!")
            self.alarm_flood_active[device_id] = True

            # Send flood notification (modify this to your preferred method)
            message_content = f"Alarm flood detected on device {device_id}!"
            asyncio.create_task(
                self.app.send_email_notification(message_content, "admin@example.com")
            )

            # Schedule flood deactivation
            asyncio.create_task(self.deactivate_alarm_flood(device_id))
    except BACpypesError as e:
        _log.error(f"BACpypes error during alarm flood detection: {e}")

                              
# ******************************************************************************


class BACeeApp(Application):
    def __init__(self, *args, **kwargs):
        """Initializes the BACeeApp with local device settings, network configuration, and BBMDs."""
    
        super().__init__(*args, **kwargs)  # Initialize the base BIPSimpleApplication
        self._log = ModuleLogger(globals())  # BACpypes ModuleLogger for this class
        self.subscriptions = {}  # Dictionary to store active subscriptions
        self.discovered_devices = {}  # Dictionary to track discovered devices
        self.registered_devices = {}
    
        # Load configuration from JSON file
        self.topology_file = "network_topology.json"  
        with open(self.topology_file, "r") as f:
            config = json.load(f)
        
        # Local Device Configuration
        local_device_config = config.get("local_device", {})
        self.this_device = LocalDeviceObject(
            objectName=local_device_config.get("device_name", "MyDevice"),
            objectIdentifier=('device', local_device_config.get("device_id", 123)),
            maxApduLengthAccepted=1024,
            segmentationSupported='segmentedBoth',
        )
    
        # Network Configuration
        self.local_address = local_device_config.get("local_address", "192.168.1.100/24")
        broadcast_address = local_device_config.get("broadcast_address", "255.255.255.255")
        bbmd_address = local_device_config.get("bbmd_address")
    
        # Network Service Access Point and Element
        self.nsap = NetworkServiceAccessPoint()
        self.nse = NetworkServiceElement()
        bind(self.nse, self.nsap)
        bind(self, self.nse)
    
        # Bind the local address
        try:
            self.this_application = NormalApplication(self, Address(self.local_address))
        except Exception as e:
            _log.error("Error while binding to local address, is it a valid IP/Subnet?: %r", e)
            sys.exit(1)
        
        # Initialize DeviceInfoCache
        self.deviceInfoCache = DeviceInfoCache(self.this_device)
    
        # BBMD Configuration
        self.bbmds = {} 
        self.bbmd_round_robin_index = 0
        self.load_bbmd_configurations()
    
    
        # Other Initializations
        self.acknowledged_alarms = set()
        self.active_alarms = {}
        self.cov_history = defaultdict(lambda: defaultdict(list))
        self.reminder_interval = 60  # Send reminders every 60 seconds
    
        # Database Connection and Table Creation
        self.db_conn = sqlite3.connect('bacee.db')
        self.create_tables()
    
        # Create Helper Classes (PropertyReader, PropertyWriter, AlarmManager)
        self.property_reader = PropertyReader(self)
        self.property_writer = PropertyWriter(self)
        self.alarm_manager = AlarmManager(self)
           
    def create_tables(self):
        """Creates the necessary tables in the database if they don't exist."""
        _log.debug("Creating database tables (if not exist)...")
    
        with self.db_conn:  # Use a context manager for automatic transaction handling
            cursor = self.db_conn.cursor()
            
            # Create alarms table
            cursor.execute("""
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
                    acknowledged INTEGER DEFAULT 0,
                    cleared INTEGER DEFAULT 0
                )
            """)
    
            # Create cov_notifications table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cov_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    device_id INTEGER,
                    object_id TEXT,
                    property_id TEXT,
                    value TEXT
                )
            """)
    
            # Create silenced_alarms table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silenced_alarms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER,
                    object_id TEXT,
                    property_id TEXT,
                    alarm_type TEXT,
                    silence_end_time REAL
                )
            """)
    
            # Create registered_devices table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS registered_devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER UNIQUE,
                    objects_properties TEXT
                )
            """)
            
            self.db_conn.commit()
    
    async def start(self):
        """Starts the BACnet application, performs necessary setup, and discovers devices."""
        _log.info("Starting BACeeApp...")
        try:
            await self.async_start()
            _log.info(f"BACnet application started for device: {self.localDevice.objectName}")
        except Exception as e:
            _log.error(f"Failed to start BACeeApp: {e}")
            return
    
        # BBMD Discovery and Registration (if needed)
        if self.bbmds:
            discovered_bbmds = []
            for bbmd in self.bbmds.values():
                _log.info(f"Discovering devices using BBMD at {bbmd.address}")
                discovered = await bbmd.discover_bbmds()
                discovered_bbmds.extend(discovered)
    
            selected_bbmd = await bbmd.select_bbmd(discovered_bbmds)
            if selected_bbmd:
                await selected_bbmd.register_foreign_device()
                _log.info(f"Registered with BBMD at {selected_bbmd.address}")
            else:
                _log.warning("No suitable BBMD found for registration.")
        else:
            _log.info("No BBMDs configured. Operating in local broadcast mode.")
    
        # Discover Devices
        self.discovered_devices = await self.discover_devices()
    
        # Load and Subscribe to Registered Devices
        await self.load_registered_devices()
    
        _log.info("BACeeApp is now running.")
    
    async def shutdown(self):
        """Gracefully shuts down the BACnet application."""
        _log.info("Shutting down BACeeApp...")
    
        # 1. Unsubscribe from all COV Subscriptions
        _log.debug("Unsubscribing from all COV subscriptions...")
        for subscription in list(self.subscriptions.keys()):  # Iterate over a copy of the keys
            await self.unsubscribe_cov(
                subscription.device_id, subscription.obj_id, subscription.prop_id
            )
        
        # 2. Cancel Pending Tasks
        _log.debug("Canceling pending tasks...")
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():  # Avoid canceling the current task
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    _log.debug(f"Cancelled task: {task}")
    
        # 3. Stop Topology Watcher
        if hasattr(self, 'bbmd') and hasattr(self.bbmd, 'topology_watcher'):
            _log.debug("Stopping topology watcher task...")
            self.bbmd.topology_watcher.cancel()
    
        # 4. Close Database Connection
        _log.debug("Closing database connection...")
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
    
        # 5. Stop the BACnet Application
        _log.debug("Stopping BACnet application...")
        stop()
    
        _log.info("BACeeApp shutdown complete.")
        
    async def load_registered_devices(self):
        """Loads registered device information from the database and sets up subscriptions."""
        _log.info("Loading registered devices from database...")
    
        try:
            with self.db_conn:  
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT device_id, objects_properties FROM registered_devices")
                rows = cursor.fetchall()
    
            for device_id, objects_properties_json in rows:
                objects_properties = json.loads(objects_properties_json)
                self.registered_devices[device_id] = objects_properties
    
                # Subscribe to properties
                for obj_id_str, prop_ids in objects_properties.items():
                    obj_id = eval(obj_id_str)  # Convert string representation back to tuple
                    for prop_id in prop_ids:
                        _log.info(f"Subscribing to {obj_id}.{prop_id} on device {device_id}")
                        subscription = Subscription(device_id, obj_id, prop_id)
                        subscription.alarms = []
                        self.subscriptions[subscription] = subscription
                        await self.subscribe_cov(subscription) 
            _log.info("Registered devices loaded and subscriptions initiated.")
    
        except sqlite3.Error as e:
            _log.error(f"Error loading registered devices: {e}")
        except Exception as e:
            _log.exception(f"Unexpected error loading registered devices: {e}")
    
    def save_registered_device(self, device_id, objects_properties):
        """Saves registered device information to the database."""
        _log.debug(f"Saving registered device {device_id} with properties {objects_properties} to the database")
    
        try:
            objects_properties_json = json.dumps(objects_properties)
    
            with self.db_conn:  # Use context manager for automatic transaction handling
                cursor = self.db_conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO registered_devices (device_id, objects_properties) VALUES (?, ?)",
                    (device_id, objects_properties_json),
                )
    
            _log.info(f"Registered device {device_id} saved to database")
    
        except sqlite3.Error as e:
            _log.error(f"Error saving registered device {device_id} to database: {e}")
            
    # do_IAmRequest
    async def do_IAmRequest(self, apdu):
        """Handles incoming I-Am requests, including device registration."""
    
        _log.debug(f"Received I-Am Request from {apdu.pduSource}")
    
        # Add or Update Device in Cache
        device_info = self.deviceInfoCache.get_device_info(apdu.pduSource)
        if device_info is None:
            device_info = DeviceInfo(apdu.pduSource, device_id=apdu.iAmDeviceIdentifier[1])
        device_info.update_device_info(apdu)
        self.deviceInfoCache.add_device_info(device_info)
        device_id = device_info.device_identifier[1]
        self.discovered_devices[device_id] = apdu.pduSource
    
        # Check for Registration Information
        registration_data = apdu.vendorProprietaryValue  # Assuming registration data is here (adjust if needed)
        if registration_data and self.validate_registration(registration_data, device_id):
            _log.info(f"Device {device_id} registered successfully.")
    
            # Store registration info in database
            self.save_registered_device(device_id, registration_data.get('objects_properties', {}))
    
            # Subscribe to properties from registration data
            for obj_id, prop_ids in registration_data.get('objects_properties', {}).items():
                obj_type, obj_instance = obj_id
                for prop_id in prop_ids:
                    subscription = Subscription(device_id, (obj_type, obj_instance), prop_id)
                    subscription.alarms = []  # Assuming no alarms for registered devices by default
                    self.subscriptions[(device_id, (obj_type, obj_instance), prop_id)] = subscription
                    await self.subscribe_cov(subscription)
        else:
            _log.warning(
                f"Invalid or inconsistent registration data from device {device_id}: {registration_data}"
            )
    
    async def do_WhoIsRequest(self, apdu: WhoIsRequest) -> None:
        """Responds to Who-Is requests to announce the local device if it matches the request parameters."""
        _log.debug(f"Received Who-Is Request from {apdu.pduSource}")
    
        # 1. Check if Local Device is Initialized
        if self.localDevice is None or not self.localDevice.objectName or not self.localAddress:
            _log.warning("Local device not fully initialized. Skipping Who-Is response.")
            return
    
        # 2. Extract Range Limits
        low_limit = apdu.deviceInstanceRangeLowLimit
        high_limit = apdu.deviceInstanceRangeHighLimit
    
        # 3. Get Local Device ID
        device_id = self.localDevice.objectIdentifier[1]
    
        # 4. Respond Only if Within Range or Unrestricted
        if (low_limit is None and high_limit is None) or (low_limit <= device_id <= high_limit):
            _log.debug(f"Responding to Who-Is with I-Am for device {device_id}")
    
            try:
                self.response(
                    IAmRequest(
                        iAmDeviceIdentifier=self.localDevice.objectIdentifier,
                        maxApduLengthAccepted=self.localDevice.maxApduLengthAccepted,
                        segmentationSupported=self.localDevice.segmentationSupported,
                        vendorID=self.localDevice.vendorIdentifier,
                    )
                )
            except Exception as e:  # Catch any unexpected errors during response
                _log.error(f"Error sending I-Am response: {e}")
        else:
            _log.debug(f"Local device ID {device_id} outside of requested range. Not responding.")
    
    
    
    # discover_devices
    async def discover_devices(self):
        """Discovers BACnet devices on the network using either local broadcast or a BBMD."""
        _log.info("Starting device discovery process...")
    
        discovered_devices = {}  # Use a dictionary to track discovered devices
    
        def callback(address, device_id):
            """Callback function to handle discovered device information."""
            _log.info(f"Discovered device: {device_id} at {address}")
            discovered_devices[device_id] = address
            device_info = DeviceInfo(address, device_id=device_id)
            self.deviceInfoCache.add_device_info(device_info)
    
        try:
            # Prioritize discovery through BBMD if available and registered
            for bbmd in self.bbmds.values():
                if bbmd.is_available:
                    _log.info(f"Attempting discovery via BBMD at {bbmd.address}")
                    await self.who_is(remoteStation=bbmd.address, callback=callback)
                    break  # Exit the loop after discovering devices through the first available BBMD
            else:  # No available BBMD found
                _log.info("Attempting discovery using local broadcast")
                await self.who_is(callback=callback)  
    
            await asyncio.sleep(5)  # Allow time for responses to arrive
    
        except BACpypesError as e:
            _log.error(f"Error during device discovery: {e}")
    
        finally:
            self.discovered_devices = discovered_devices 
        return discovered_devices
    
     
async def validate_registration(self, registration_data, device_id):
    """Validates registration data against the device's capabilities and the database."""

    _log.debug(f"Validating registration data for device {device_id}: {registration_data}")

    try:
        # Validate device ID type
        if not isinstance(device_id, int):
            raise ValueError(f"Invalid device ID: {device_id}")

        # 1. Validate Object and Property Identifiers Against Device
        for obj_id_str, prop_ids in registration_data.get('objects_properties', {}).items():
            obj_id = eval(obj_id_str)  # Convert string back to tuple 
            for prop_id in prop_ids:
                if not await self.validate_object_and_property(device_id, obj_id, prop_id):
                    _log.warning(f"Invalid object or property in registration data for device {device_id}: {obj_id}.{prop_id}")
                    return False  # Exit early on invalid data

        # 2. Retrieve Existing Registration Data (if any) from Database
        with self.db_conn:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT objects_properties FROM registered_devices WHERE device_id = ?",
                (device_id,),
            )
            row = cursor.fetchone()

        if row:
            # 3. Compare Against Database (if entry exists)
            db_data = json.loads(row[0])
            if db_data != registration_data.get('objects_properties', {}):
                _log.warning(f"Inconsistent registration data in database for device {device_id}. Overwriting with new data.")
                with self.db_conn:  # Use context manager to ensure atomic update
                    cursor.execute(
                        "UPDATE registered_devices SET objects_properties = ? WHERE device_id = ?",
                        (json.dumps(registration_data.get('objects_properties', {})), device_id),
                    )
            else:
                _log.debug(f"Registration data for device {device_id} matches database entry.")
        else:
            # 4. Insert into Database (if no existing entry)
            _log.info(f"No existing registration data found for device {device_id}. Saving new data.")
            with self.db_conn:
                cursor.execute(
                    "INSERT INTO registered_devices (device_id, objects_properties) VALUES (?, ?)",
                    (device_id, json.dumps(registration_data.get('objects_properties', {}))),
                )
    
    except sqlite3.Error as e:
        _log.error(f"Error during registration validation: {e}")
        return False
    except (KeyError, ValueError, TypeError) as e:
        _log.error(f"Invalid registration data format: {e}")
        return False
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Unexpected error during registration validation: {e}")
        return False

    return True  
        
async def validate_object_and_property(self, device_id, obj_id, prop_id):
    """Validates if the object and property exist on the device using BACpypes3 services."""
    _log.debug(f"Validating object {obj_id} and property {prop_id} on device {device_id}")

    try:
        # 1. Read Object List
        object_list = await self.get_object_list(device_id)
        if obj_id not in object_list:
            _log.warning(f"Object {obj_id} not found on device {device_id}")
            return False

        # 2. Read Property List for the Object
        property_list_result = await self.property_reader.read_property(device_id, obj_id, 'propertyList')
        if not isinstance(property_list_result, ReadPropertyACK):
            _log.error(f"Failed to read propertyList for object {obj_id} on device {device_id}")
            return False

        property_list = [prop.identifier for prop in property_list_result.propertyValue[0].value]

        # 3. Check if Property is in the List
        if prop_id not in property_list:
            _log.warning(f"Property {prop_id} not found for object {obj_id} on device {device_id}")
            return False

        return True  # Object and property are valid

    except (CommunicationError, TimeoutError, BACpypesError) as e:
        _log.error(f"Error validating object and property on device {device_id}: {e}")
        return False
    
    async def check_writable_properties(self, device_id, object_type, object_instance):
        """Checks writable properties for a given object on a device."""
        _log.info(
            f"Checking writable properties for object {object_type}:{object_instance} on device {device_id}"
        )
        obj_id = (object_type, object_instance)
        try:
            result = await self.property_reader.read_multiple_properties(
                device_id, obj_id, ["propertyList", "all"]  # Read "all" to get property details
            )
    
            if result is None:
                _log.error(
                    f"Failed to read properties for object {obj_id} on device {device_id}"
                )
                return []
    
            property_list = [
                prop.identifier
                for prop in result["propertyList"][0]
                if isinstance(prop, PropertyIdentifier)
            ]
    
            writable_properties = []
            for prop_id in property_list:
                # Check the 'writable' attribute in the Property object
                prop_value = result.get(("all", prop_id), None)
                if prop_value and isinstance(prop_value[0], Property) and prop_value[0].writable:
                    writable_properties.append(prop_id)
    
            return writable_properties
    
        except (CommunicationError, TimeoutError, BACpypesError) as e:  # Catch more specific BACpypes errors
            _log.error(f"Error checking writable properties for device {device_id}: {e}")
            return []
    
    # check_property_writable
    async def check_property_writable(self, device_id, obj_id, property_identifier):
        """Check if a property is writable by reading its property description."""
        _log.debug(
            f"Checking if property {obj_id}.{property_identifier} is writable on device {device_id}"
        )
        try:
            # Read Property Description
            read_result = await self.property_reader.read_property(
                device_id, obj_id, "propertyDescription"
            )
    
            if read_result is None or not isinstance(read_result, ReadPropertyACK):
                _log.error(
                    f"Failed to read property description for {obj_id}.{property_identifier} on device {device_id}"
                )
                return None
    
            # Extract and Check Property Description
            property_description = read_result.propertyValue[0]
            
            # Determine Writability
            if hasattr(property_description, "writable") and property_description.writable:
                _log.debug(
                    f"Property {property_identifier} is writable for object {obj_id} on device {device_id}"
                )
                return True  # Property is writable
            else:
                _log.debug(
                    f"Property {property_identifier} is not writable for object {obj_id} on device {device_id}"
                )
                return False  # Property is not writable
    
        except (CommunicationError, TimeoutError) as e:
            _log.error(f"Communication error with device {device_id}: {e}")
            return None  # Return None on error to indicate failure
    
    
    # get_object_list
async def get_object_list(self, device_id):
    """Retrieves object list for a device using ReadPropertyMultiple, utilizing the cache."""

    _log.debug(f"Retrieving object list for device {device_id}")

    device_info = self.deviceInfoCache.get_device_info(device_id)
    if device_info is None:
        _log.error(f"Device with ID {device_id} not found.")
        return None
    
    # Check the device cache for the object list
    if device_info.object_list:
        return device_info.object_list

    try:
        result = await self.property_reader.read_multiple_properties(
            device_id, ("device", device_id), ["objectList"]
        )
        if result is not None and "objectList" in result:
            object_list = [item.objectIdentifier for item in result["objectList"][0].value]
            # Update DeviceInfoCache
            device_info.object_list = object_list
            return object_list
        else:
            _log.error(f"Failed to read object list for device {device_id}")
            return None

    except (CommunicationError, TimeoutError, BACpypesError) as e:  # Catch specific BACpypes errors
        _log.error(f"Error reading object list from device {device_id}: {e}")
        return None
    
    
    # iter_objects
    async def iter_objects(self, device_id):
        """Asynchronously iterates over all objects in a device."""
        _log.debug(f"Iterating over objects for device {device_id}")
        async for obj_id in self.get_object_list(device_id):
            yield obj_id
    
    
async def do_ConfirmedCOVNotification(self, apdu: ConfirmedCOVNotificationRequest):
    """Handles incoming confirmed COV notifications."""
    _log.debug(f"Received ConfirmedCOVNotification from {apdu.initiatingDeviceIdentifier}")

    try:
        device_id = apdu.initiatingDeviceIdentifier
        device_info = self.deviceInfoCache.get_device_info(device_id)

        if not device_info:
            _log.warning(f"Received COV notification from unknown device: {device_id}")
            return

        obj_id = apdu.monitoredObjectIdentifier
        prop_id = apdu.monitoredPropertyIdentifier

        # Handle sequence of values if present
        if isinstance(apdu.listOfValues[0].propertyValue, SequenceOf):
            values = [element.value for element in apdu.listOfValues[0].propertyValue]
        else:
            values = apdu.listOfValues[0].propertyValue

        _log.info(f"Received COV notification from {device_id}: {obj_id}.{prop_id} = {values}")

        # Update COV history
        self.cov_history[obj_id][prop_id].append((time.time(), values))
        self.alarm_manager.save_cov_notification_to_db(device_id, obj_id, prop_id, values)

        # Handle objectList changes (optional)
        if prop_id == "objectList":
            _log.info(
                f"Object list changed for device {device_id}, refreshing device information."
            )
            self.deviceInfoCache.remove_device_info(device_id)
            asyncio.create_task(self.discover_devices())
            return  # Exit early, no need to process further for objectList change

        # Find matching subscriptions and trigger alarm manager (if any)
        for subscription in self.subscriptions.values():
            if (
                subscription.device_id == device_id
                and subscription.obj_id == obj_id
                and subscription.prop_id == prop_id
            ):
                await self.alarm_manager.handle_cov_notification(
                    prop_id, values, subscription
                )
                
                # Send SocketIO notification (if using SocketIO)
                await socketio.emit('property_update', {
                    'deviceId': subscription.device_id,
                    'objectId': subscription.obj_id,
                    'propertyId': prop_id,
                    'value': values
                })  

    except (DecodingError, SegmentationError) as e:
        _log.error(f"Error decoding or segmenting COV notification: {e}")
    except BACpypesError as e:
        _log.error(f"BACpypes error processing COV notification: {e}")
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Unexpected error processing COV notification: {e}")

    
async def subscribe_cov(self, subscription: Subscription, renew: bool = False, timeout: int = 5):
    """Subscribes to or renews a COV subscription for the given property."""

    _log.debug(
        f"{'Renewing' if renew else 'Subscribing to'} COV for "
        f"{subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}"
    )

    device_info = self.deviceInfoCache.get_device_info(subscription.device_id)
    if not device_info:
        _log.error(
            f"Device with ID {subscription.device_id} not found. Cannot subscribe."
        )
        return

    try:
        # Create the SubscribeCOVRequest
        cov_request = SubscribeCOVRequest(
            subscriberProcessIdentifier=self.localDevice.objectIdentifier[1],
            monitoredObjectIdentifier=subscription.obj_id,
            issueConfirmedNotifications=subscription.confirmed_notifications,
            lifetimeInSeconds=subscription.lifetime_seconds,
        )
        if subscription.cov_increment is not None:
            cov_request.covIncrement = subscription.cov_increment
        elif subscription.cov_increment_percentage is not None:
            cov_request.covIncrementPercentage = subscription.cov_increment_percentage

        # Create the task and store it in the subscription
        task = asyncio.create_task(self._process_subscription(subscription, cov_request))
        subscription.cov_task = task

        _log.info(f"COV subscription for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id} {'renewed' if renew else 'created'} successfully.")
        return True
    except asyncio.TimeoutError:
        _log.error(f"Subscription timeout for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}")
    except (CommunicationError, BACpypesError) as e:  # Catch specific BACpypes errors
        _log.error(f"Error subscribing to COV: {e}")
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Unexpected error during COV subscription: {e}")
    
    subscription.active = False # If any error happens, disable the subscription
    return False  # Failed to subscribe


async def _process_subscription(self, subscription: Subscription, cov_request: SubscribeCOVRequest):
    """Handles the async context for COV subscriptions and renewals."""

    device_info = self.deviceInfoCache.get_device_info(subscription.device_id)
    async with self.change_of_value(
        device_info.address,
        subscription.obj_id,
        subscription.prop_id,
        subscription.confirmed_notifications,
        subscription.lifetime_seconds,
        cov_request,
    ) as scm:
        subscription.context_manager = scm

        # Handle COV Notifications
        while not scm.is_fini:
            try:
                property_identifier, property_value = await scm.get_value()
                _log.info(f"Received COV notification: {subscription.obj_id}.{property_identifier} = {property_value}")
                await self.alarm_manager.handle_cov_notification(property_identifier, property_value, subscription)
                await socketio.emit('property_update', {'deviceId': subscription.device_id, 'objectId': subscription.obj_id, 'propertyId': property_identifier, 'value': property_value})
            except Exception as e:
                _log.exception(f"Error processing COV notification for subscription {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}: {e}")
                break  # Exit loop on error

    _log.info(f"Subscription for {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id} ended.")
    subscription.active = False  # Mark subscription as inactive when it ends

    
    async def manage_subscriptions(self):
        """Manages active subscriptions, including renewals."""
        _log.debug("Starting subscription management task.")
        while True:
            try:
                _log.debug("Checking subscriptions for renewal...")
                for subscription in list(self.subscriptions.keys()):  # Iterate over a copy in case subscriptions are removed
                    if subscription.active and subscription.lifetime_seconds is not None:
                        remaining_lifetime = subscription.lifetime_seconds - (time.time() - subscription.last_renewed_time)
                        if remaining_lifetime < 60:  # Renew a minute before expiration
                            _log.info(f"Renewing subscription: {subscription.obj_id}.{subscription.prop_id} on device {subscription.device_id}")
                            await subscription.renew_subscription(self)
            except Exception as e:
                _log.error(f"Error managing subscriptions: {e}")
    
            await asyncio.sleep(60)  # Check for renewal every minute (adjust as needed)
    
    # unsubscribe_cov
    async def unsubscribe_cov(self, device_id, obj_id, prop_id):
        """Unsubscribes from COV notifications for a specific property."""
        _log.info(f"Unsubscribing from COV for {obj_id}.{prop_id} on device {device_id}")
        subscription_key = (device_id, obj_id, prop_id)
    
        try:
            if subscription_key in self.subscriptions:
                subscription = self.subscriptions[subscription_key]
                if subscription.cov_task:  # Cancel the task associated with the subscription
                    subscription.cov_task.cancel()
                    
                del self.subscriptions[subscription_key] 
            else:
                _log.warning(f"Subscription not found for {obj_id}.{prop_id} on device {device_id}")  # Log a warning if not found
        except Exception as e:
            _log.error(f"Error unsubscribing from COV: {e}")
                
    # Request IO
async def request_io(self, request, timeout=5, retries=3):
    """Sends a BACnet request and handles retries with exponential backoff."""
    _log.debug(f"Sending BACnet request: {request}")

    # Determine the destination address (BBMD or broadcast)
    if self.bbmds and not request.pduDestination.is_broadcast:
        selected_bbmd = self.select_bbmd(list(self.bbmds.values()))
        if selected_bbmd and selected_bbmd.is_available():
            request.pduDestination = selected_bbmd.address
            _log.debug(f"Routing request through BBMD: {selected_bbmd.address}")
        else:
            _log.warning("No available BBMD found. Falling back to broadcast.")
            request.pduDestination = Address(BROADCAST_ADDRESS)
    else:
        request.pduDestination = Address(BROADCAST_ADDRESS)

    # Retry logic with exponential backoff
    for attempt in range(retries + 1):
        try:
            response = await asyncio.wait_for(self.request(request), timeout)
            if response:
                _log.debug(f"Received response from {request.pduDestination}: {response}")
                return response
            else:
                _log.warning(f"No response received from {request.pduDestination} in attempt {attempt + 1}/{retries + 1}")
        except asyncio.TimeoutError:
            _log.warning(f"Timeout error for request to {request.pduDestination}, attempt {attempt + 1} of {retries + 1}.")
        except (CommunicationError, BACpypesError) as e:
            _log.error(f"Error sending request to {request.pduDestination}: {e}")
            return None  # Return immediately if a communication error happens

        if attempt < retries:  
            await asyncio.sleep(2 ** attempt) 

    _log.error(f"Request to {request.pduDestination} failed after {retries} retries.")
    return None  # Indicate failure after all retries



    # do_RouterAvailable
    def do_RouterAvailable(self, apdu):
        """Called when a router becomes available."""
        _log.info(f"Router available: {apdu.pduSource}")
        # You might want to add logic here to handle the available router, e.g., update BBMD status.
    
    # do_RouterUnavailable
    def do_RouterUnavailable(self, apdu):
        """Called when a router becomes unavailable."""
        _log.info(f"Router unavailable: {apdu.pduSource}")
        # You might want to add logic here to handle the unavailable router, e.g., update BBMD status.
    
    # load_bbmd_configurations
    def load_bbmd_configurations(self):
        """Loads BBMD configurations from the topology file and creates BBMD objects."""
        _log.debug("Loading BBMD configurations...")
    
        try:
            with open(self.topology_file, "r") as f:
                topology_data = json.load(f)
                for bbmd_config in topology_data.get("BBMDs", []):
                    address = bbmd_config["address"]
                    db_file = "your_database_file.db"  # Make sure to replace with your actual database file
                    bbmd_name = bbmd_config.get("name")
                    bbmd = BBMD(address, db_file, self.topology_file, bbmd_name)
                    self.bbmds[address] = bbmd
        except (FileNotFoundError, json.JSONDecodeError) as e:
            _log.error(f"Error loading BBMD configurations: {e}")

    
    async def schedule_task(self):
        """Asynchronously executes scheduled tasks."""
        _log.debug("Starting scheduled task execution")
    
        while True:
            try:
                cursor = self.db_conn.cursor()
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current time
                cursor.execute(
                    "SELECT id, device_id, object_id, property_id, value, active FROM schedules "
                    "WHERE scheduled_time <= ?", (now,)
                )
    
                for row in cursor.fetchall():
                    task_id, device_id, obj_id, prop_id, value, active = row
                    device_id = int(device_id)
                    obj_id = eval(obj_id)  # Convert string to tuple
    
                    if active:  # Check if the task is active
                        _log.info(f"Executing scheduled task: {obj_id}.{prop_id} = {value} on device {device_id}")
                        try:
                            await self.property_writer.write_property(device_id, obj_id, prop_id, value)
                        except Exception as e:
                            _log.error(f"Error executing scheduled task: {e}")
    
                        # Update task status if needed (e.g., mark as completed or inactive)
                        cursor.execute(
                            "UPDATE schedules SET active = 0 WHERE id = ?", (task_id,)
                        )
    
                self.db_conn.commit()  # Commit changes after processing all tasks
    
            except sqlite3.Error as e:
                _log.error(f"Error executing scheduled task: {e}")
                self.db_conn.rollback()  # Rollback on error
    
            # Sleep for one minute
            await asyncio.sleep(60)


# ******************************************************************************

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
        """Renews the COV subscription, handling potential errors gracefully."""

        subscription_info = f"{self.obj_id}.{self.prop_id}"  

        if not self.active:
            _log.warning(f"Attempting to renew inactive subscription: {subscription_info}")
            return

        _log.info(f"Renewing COV subscription: {subscription_info}")

        subscription_request = SubscribeCOVRequest(
            subscriberProcessIdentifier=app.localDevice.objectIdentifier[1],  # Assumes local device object exists
            monitoredObjectIdentifier=self.obj_id,
            issueConfirmedNotifications=self.confirmed_notifications,
            lifetimeInSeconds=self.lifetime_seconds,
        )
        if self.cov_increment is not None:
            subscription_request.covIncrement = self.cov_increment
        elif self.cov_increment_percentage is not None:
            subscription_request.covIncrementPercentage = self.cov_increment_percentage

        try:
            response = await asyncio.wait_for(app.subscribe_cov(subscription_request, self), timeout=timeout)
            if isinstance(response, SubscribeCOVRequest):  # Successful renewal returns original request
                _log.info(f"Successfully renewed subscription: {subscription_info}")
            else:
                _log.error(f"Unexpected response received when trying to renew subscription: {subscription_info}")

        except (Error, Reject, Abort) as e:  # Catch specific BACpypes3 errors
            _log.error(f"Error renewing subscription: {subscription_info} - {e.errorClass} - {e.errorCode}")
            self.active = False
        except TimeoutError as e:
            _log.error(f"Timeout renewing subscription: {subscription_info} after {timeout} seconds")
            self.active = False
        except Exception as e:  # Catch-all for unexpected errors
            _log.exception(f"Unexpected error renewing subscription: {subscription_info} - {e}")
            self.active = False

            # ******************************************************************************

        class PropertyReader:
            def __init__(self, app: BACeeApp):
                self.app = app

            async def read_property(self, device_id, obj_id, prop_id):
                """Reads a single property from a BACnet device, utilizing the DeviceInfoCache.

                Args:
                    device_id: The ID of the BACnet device.
                    obj_id: The object identifier (e.g., 'analogInput:1').
                    prop_id: The property identifier (e.g., 'presentValue').

                Returns:
                    The property value if successful, or None if an error occurred or the device/property is not found.
                """

                device_info = self.app.deviceInfoCache.get_device_info(device_id)
                if not device_info:
                    _log.error(f"Device with ID {device_id} not found in cache.")
                    return None

                # Check if the property is already cached
                cached_value = device_info.get_object_property(obj_id, prop_id)
                if cached_value is not None:
                    _log.debug(f"Property {obj_id}.{prop_id} found in cache for device {device_id}: {cached_value}")
                    return cached_value

                # Property not cached, read from device
                _log.debug(f"Reading property {obj_id}.{prop_id} from device {device_id}")
                request = ReadPropertyRequest(objectIdentifier=obj_id, propertyIdentifier=prop_id)
                request.pduDestination = device_info.address

                try:
                    response = await self.app.request(request)  # Assuming 'app' has a 'request' method
                    if isinstance(response, ReadPropertyACK):
                        value = response.propertyValue
                        # Cache the value in DeviceInfoCache
                        device_info.set_object_property(obj_id, prop_id, value)
                        _log.debug(f"Read property {obj_id}.{prop_id} from device {device_id}: {value}")
                        return value
                    else:
                        _log.error(
                            f"Error reading property {obj_id}.{prop_id} on device {device_id}: Unexpected response type {response}")

                except (CommunicationError, TimeoutError) as e:
                    _log.error(f"Communication error reading property {obj_id}.{prop_id} from device {device_id}: {e}")

                return None  # Return None on error or if property not found

            async def read_multiple_properties(self, device_id, obj_id_prop_id_list):
                """Reads multiple properties from multiple objects on a BACnet device using ReadPropertyMultiple.

                Args:
                    device_id: The ID of the BACnet device.
                    obj_id_prop_id_list: A list of tuples, each containing an object ID and a property ID to read.

                Returns:
                    A dictionary where keys are (object ID, property ID) tuples and values are the corresponding property values.
                    Returns None if an error occurs or the device is not found.
                """

                device_info = self.app.deviceInfoCache.get_device_info(device_id)
                if not device_info:
                    _log.error(f"Device with ID {device_id} not found in cache.")
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
                    response = await self.app.request(request)  # Assuming 'app' has a 'request' method
                    if isinstance(response, ReadPropertyMultipleACK):
                        values = {}
                        for obj_prop_list in response.values:
                            for prop_value in obj_prop_list:
                                values[(prop_value.objectIdentifier, prop_value.propertyIdentifier)] = prop_value.value

                        # Update the cache with the read values
                        device_info.update_properties(values)  # Update the cache
                        return values

                    else:
                        _log.error(
                            f"Error reading multiple properties from device {device_id}: Unexpected response type {response}")

                except (CommunicationError, TimeoutError) as e:
                    _log.error(f"Communication error with device {device_id}: {e}")
                    # You might want to handle the error more specifically, e.g., retry or raise a custom exception.

                return None  # Return None on error or unexpected response

# ******************************************************************************

        class PropertyWriter(BIPSimpleApplication,
                             WritePropertyService):  # Inherit from BIPSimpleApplication and WritePropertyService
            def __init__(self, *args, **kwargs):
                BIPSimpleApplication.__init__(self, *args, **kwargs)

            async def write_property(self, device_id, obj_id, prop_id, value, priority=None):
                """Writes a value to a BACnet property using BACpypes3's built-in WritePropertyService."""
                try:
                    device_info = self.app.deviceInfoCache.get_device_info(device_id)
                    if not device_info:
                        _log.error(f"Device with ID {device_id} not found. Cannot write property.")
                        return

                    # Convert value to appropriate BACnet type if needed (same logic as before)

                    request = WritePropertyRequest(
                        objectIdentifier=obj_id,
                        propertyIdentifier=prop_id,
                        propertyArrayIndex=None,
                        propertyValue=value,
                        priority=priority,
                    )

                    request.pduDestination = device_info.address
                    response = await self.request(request)  # Use BIPSimpleApplication's request method

                    if response.errorClass is None and response.errorCode is None:
                        _log.info(f"Successfully wrote {value} to {obj_id}.{prop_id}")
                        device_info.set_object_property(obj_id, prop_id, value)
                    else:
                        error_class = response.errorClass or "UnknownError"
                        error_code = response.errorCode or "UnknownCode"
                        _log.error(f"Failed to write {value} to {obj_id}.{prop_id}: {error_class} - {error_code}")
                except Exception as e:
                    _log.exception(f"Error writing {value} to {obj_id}.{prop_id}: {e}")

            async def write_multiple_properties(self, device_id, obj_id, prop_values, priority=None):
                """Writes multiple properties to a BACnet object using BACpypes3's WritePropertyMultipleService.

                Args:
                    device_id: The ID of the BACnet device.
                    obj_id: The object identifier (e.g., 'analogValue:1').
                    prop_values: A dictionary where keys are property identifiers and values are the values to write.
                    priority: (Optional) The priority of the write.

                Returns:
                    None
                """
                device_info = self.app.deviceInfoCache.get_device_info(device_id)
                if not device_info:
                    _log.error(f"Device with ID {device_id} not found. Cannot write properties.")
                    return

                # Validate property values if necessary
                for prop_id, value in prop_values.items():
                    if not self.validate_property_value(obj_id, prop_id, value):
                        _log.error(f"Invalid value {value} for property {prop_id} of object {obj_id}")
                        return

                write_access_spec_list = []
                for prop_id, value in prop_values.items():
                    prop_data_type = get_datatype(obj_id[0], prop_id)
                    if isinstance(value, list):  # Handle list values as ArrayOf
                        value = ArrayOf(prop_data_type, value)
                    else:
                        value = prop_data_type(value)  # Convert individual values to BACnet type
                    write_access_spec_list.append(
                        WritePropertyMultipleResult(
                            propertyIdentifier=prop_id,
                            propertyValue=value,
                            priority=priority,
                        )
                    )

                request = WritePropertyMultipleRequest(
                    objectIdentifier=obj_id,
                    listOfWriteAccessSpecs=[write_access_spec_list]  # List of lists for multiple properties
                )
                request.pduDestination = device_info.address
                try:
                    response = await self.multiple_write_property_request(
                        request)  # Use WritePropertyMultipleService's request method

                    for result in response:
                        if result.propertyAccessError is not None:  # Check for errors in the result
                            error_class = result.propertyAccessError.errorClass or "UnknownError"
                            error_code = result.propertyAccessError.errorCode or "UnknownCode"
                            _log.error(
                                f"Error writing property {prop_id} of object {obj_id}: {error_class} - {error_code}"
                            )
                        else:
                            _log.info(f"Successfully wrote to property {prop_id} of object {obj_id}")
                            device_info.set_object_property(obj_id, prop_id, prop_values[prop_id])  # Update the cache

                except Exception as e:
                    _log.exception(f"Error writing multiple properties to {obj_id} on {device_id}: {e}")
                    return

# ******************************************************************************


class TrendAnalyzer:
    def __init__(self, app: BACeeApp):
        self.app = app

    async def analyze_trends(self, obj_id, prop_id, num_points=10, plot=True):
        """Analyzes and optionally plots the trend for the given object and property.

        Args:
            obj_id: The object identifier.
            prop_id: The property identifier.
            num_points: (Optional) The number of data points to consider for analysis (default: 10).
            plot: (Optional) Whether to plot the trend (default: True).
        """

        history = self.app.cov_history.get(obj_id, {}).get(prop_id, [])
        if len(history) < 2:
            _log.warning("Not enough data for trend analysis.")
            return

        # Limit the number of data points for analysis
        timestamps, values = zip(*history[-num_points:])

        # Convert to NumPy arrays
        x = np.array(timestamps)
        y = np.array(values)

        # Calculate trend line (linear regression)
        coeffs = np.polyfit(x, y, 1)  # Fit a first-degree polynomial (linear regression)
        trend_line = np.poly1d(coeffs)

        _log.info(f"Trend for {obj_id}.{prop_id}: {trend_line}")  # Log the trend line equation

        if plot:
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

# Database Interaction Functions
def execute_query(db_path, query, parameters=None):
    """Executes a SQL query with enhanced error handling and logging."""
    _log.debug(f"Executing SQL query: {query}")
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.fetchall()
    except sqlite3.Error as e:
        _log.error(f"Database error: {e}")
        raise  # Re-raise the exception for the caller to handle


def fetch_data(db_path, query, parameters=None):
    """Fetches data from the database with error handling."""
    _log.debug(f"Fetching data from database with query: {query}")
    try:
        return execute_query(db_path, query, parameters)
    except sqlite3.Error as e:
        _log.error(f"Error fetching data: {e}")
        return None


# Helper functions for CLI interactions
async def async_input(prompt: str) -> str:
    """Get input from the user asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)


# handle_create_subscription
async def handle_create_subscription(app):
    """Handles the process of creating a subscription interactively."""
    _log.info("\nCreating a new subscription...")

    # Display discovered devices and get user selection
    _log.info("Discovered Devices:")
    for idx, device in enumerate(app.discovered_devices.values()):
        _log.info(f"{idx + 1}. Device ID: {device.device_id}, Name: {device.device_name}, Address: {device.address}")

    selected_device = await _get_user_selection(app.discovered_devices, "device")

    # Display available objects and get user selection
    object_list = await app.get_object_list(selected_device.device_id)
    _log.info("Available Objects:")
    for idx, obj_id in enumerate(object_list):
        _log.info(f"{idx + 1}. Object ID: {obj_id}")

    selected_object_id = await _get_user_selection(object_list, "object")

    # Display writable properties and get user selection
    property_list = await app.check_writable_properties(selected_device.device_id, *selected_object_id)
    _log.info("Writable Properties:")
    for idx, prop_id in enumerate(property_list):
        _log.info(f"{idx + 1}. Property ID: {prop_id}")

    selected_prop_id = await _get_user_selection(property_list, "property")

    # Get confirmation type and lifetime from the user
    confirmed = await _get_confirmation("Confirmed notifications? (yes/no): ")
    lifetime_seconds = await _get_positive_int("Enter lifetime in seconds (0 for indefinite): ")

    # Create the subscription and handle errors
    try:
        subscription = Subscription(selected_device.device_id, selected_object_id, selected_prop_id, confirmed, lifetime_seconds)
        await app.subscribe_cov(subscription)
    except Exception as e:
        _log.error(f"Error creating subscription: {e}")


# handle_unsubscribe
async def handle_unsubscribe(app):
    """Handles the process of unsubscribing from a subscription."""
    _log.info("\nActive Subscriptions:")
    for idx, subscription in enumerate(app.subscriptions.keys()):  # Iterate directly over subscription objects
        device = app.discovered_devices.get(subscription.device_id)
        if device:
            _log.info(
                f"{idx + 1}. Device '{device.device_name}' ({subscription.device_id}), Object '{subscription.obj_id}', Property '{subscription.prop_id}'"
            )

    selected_subscription = await _get_user_selection(app.subscriptions, "subscription")
    if not selected_subscription:
        return  # User aborted or made an invalid selection

    confirm = await async_input(f"Are you sure you want to cancel this subscription? (yes/no): ")
    if confirm.lower() == "yes":
        await app.unsubscribe_cov(selected_subscription.device_id, selected_subscription.obj_id, selected_subscription.prop_id)
    else:
        _log.info("Subscription cancellation aborted.")

# handle_acknowledge_alarm
async def handle_acknowledge_alarm(app):
    """Handles the process of acknowledging an alarm."""
    _log.info("\nActive Alarms:")
    for idx, alarm_key in enumerate(app.alarm_manager.active_alarms):
        device_id, obj_id, prop_id, alarm_type = alarm_key
        device = app.discovered_devices.get(device_id)
        if device:
            _log.info(f"{idx + 1}. Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}': {alarm_type}")

    try:
        alarm_index = int(await async_input("Select alarm to acknowledge by index: ")) - 1
        selected_alarm = list(app.alarm_manager.active_alarms)[alarm_index]
        await app.alarm_manager.acknowledge_alarm(selected_alarm)  # Call acknowledge_alarm with alarm_key
    except (IndexError, ValueError):
        _log.error("Invalid alarm selection.")
    

# Helper function for user selection
async def _get_user_selection(items, item_type):
    """Helper function to get user selection from a list of items."""
    while True:
        try:
            index = int(await async_input(f"Select {item_type} by index: ")) - 1
            if 0 <= index < len(items):
                return list(items.values())[index] if isinstance(items, dict) else items[index]
            else:
                _log.error(f"Invalid {item_type} index. Please enter a valid number.")
        except ValueError:
            _log.error(f"Invalid input. Please enter a number.")

# Helper function for confirmation
async def _get_confirmation(prompt):
    """Helper function to get yes/no confirmation from the user."""
    while True:
        confirmed_input = await async_input(prompt).lower()
        if confirmed_input in ["yes", "y"]:
            return True
        elif confirmed_input in ["no", "n"]:
            return False
        else:
            _log.error("Invalid input. Please enter 'yes' or 'no'.")

# Helper function for positive integer input
async def _get_positive_int(prompt):
    """Helper function to get a positive integer input from the user."""
    while True:
        try:
            value = int(await async_input(prompt))
            if value >= 0:
                return value
            else:
                _log.error("Invalid input. Please enter a non-negative number.")
        except ValueError:
            _log.error("Invalid input. Please enter a number.")

        
        
# ******************************************************************************


async def cli_loop(app):
    """Interactive command-line interface loop for the BACeeApp."""
    while True:
        _log.info("\nBACee Menu:")
        _log.info("1. Discover Devices")
        _log.info("2. Create Subscription")  # Updated menu item
        _log.info("3. List Subscriptions")   # Updated menu item
        _log.info("4. Unsubscribe")
        _log.info("5. View Active Alarms")
        _log.info("6. Acknowledge Alarm")
        _log.info("7. List Discovered Devices")
        _log.info("8. Quit")

        choice = await async_input("Enter your choice: ")

        try:
            choice = int(choice)  # Attempt to convert input to integer

            if choice == 1:
                await app.discover_devices()

                # Display discovered devices in a more structured format
                _log.info("\nDiscovered Devices:")
                for device_id, device in app.discovered_devices.items():
                    _log.info(
                        f"  - Device ID: {device_id}, Name: {device.device_name}, Address: {device.address}"
                    )

            elif choice == 2:
                await handle_create_subscription(app)
            
            elif choice == 3:
                _log.info("\nActive Subscriptions:")
                if not app.subscriptions:
                    _log.info("  No active subscriptions.")
                else:
                    for key, sub in app.subscriptions.items():
                        device_id, obj_id, prop_id = key
                        device = app.discovered_devices.get(device_id)
                        if device:
                            _log.info(f"  - Device '{device.device_name}' ({device_id}), Object '{obj_id}', Property '{prop_id}'")
    
            elif choice == 4:
                await handle_unsubscribe(app)

            elif choice == 5:
                await app.alarm_manager.print_active_alarms()
            elif choice == 6:
                await handle_acknowledge_alarm(app)

            elif choice == 7:
                _log.info("\nDiscovered Devices:")
                for device_id, device in app.discovered_devices.items():
                    _log.info(f"  - Device ID: {device_id}, Name: {device.device_name}, Address: {device.address}")
    
            elif choice == 8:
                _log.info("Exiting BACee CLI. Goodbye!")
                break
            else:
                _log.warning("Invalid choice. Please enter a number between 1 and 8.")
        except ValueError:
            _log.warning("Invalid input. Please enter a number.")



# ******************************************************************************


# --- Flask ---


app_flask = Flask(__name__)
socketio = SocketIO(app_flask, cors_allowed_origins="*")  
auth = HTTPBasicAuth()

# WebSocket Events

# Function to verify JWT token (replace with your actual implementation)
def verify_jwt(token):
    """Verifies the JWT token."""
    _log.debug(f"Verifying JWT token: {token}")

    try:
        jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return True
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError) as e:
        _log.warning(f"Invalid or expired JWT token: {e}")
        return False
        
# Authentication decorator for SocketIO
def authenticated_only(f):
    @wraps(f)  # Preserve function metadata
    def wrapped(*args, **kwargs):
        if not request.sid or not session.get(request.sid):
            _log.warning("Unauthorized WebSocket attempt. Disconnecting client.")
            disconnect()
        else:
            return f(*args, **kwargs)
    return wrapped
    
@auth.verify_password 
def verify_password(username, password):
    """Verifies the username and password."""
    _log.debug(f"Verifying credentials for user: {username}")

    if username == "admin" and password == "password":  
        _log.info(f"User {username} authenticated successfully.")
        return True
    else:
        _log.warning(f"Authentication failed for user: {username}")
        return False
   
SECRET_KEY = os.environ.get('SECRET_KEY', 'your_default_secret_key')  # Secret key for JWT signing
 
@socketio.on('connect')
def on_connect():
    _log.info("WebSocket client connected")
    token = request.args.get('token')
    if not token or not verify_jwt(token):
        _log.warning("Unauthorized WebSocket connection attempt")
        disconnect() 
    else:
        session[request.sid] = True
        join_room(request.sid)

        _log.info(f"User {request.sid} authenticated for WebSocket connection")
        try:
            emit('device_list', list(app.deviceInfoCache.get_device_identifiers()))  # Send initial device list
        except Exception as e:
            _log.error(f"Error emitting device list: {e}")

# on_subscribe
@socketio.on('subscribe_to_property')
@authenticated_only
async def on_subscribe(data):
    """Handles subscription requests from the WebSocket client."""

    _log.debug(f"Received subscription request: {data}")

    try:
        device_id = int(data['deviceId'])
        object_type = data['objectType']
        object_instance = int(data['objectInstance'])
        property_identifier = data['propertyIdentifier']

        obj_id = (object_type, object_instance)

        # Check if the device exists
        if device_id not in app.discovered_devices:
            raise ValueError(f"Device with ID {device_id} not found.")

        # Validate object and property
        if not app.validate_object_and_property(obj_id, [property_identifier]):
            raise ValueError(f"Invalid object or property for device {device_id}")

        # Create and store the subscription
        subscription = Subscription(device_id, obj_id, property_identifier)
        app.subscriptions[subscription] = subscription
        await app.subscribe_cov(subscription)
        _log.info(f"Subscribed to {obj_id}.{property_identifier} on device {device_id}")

        emit('subscription_success', {'message': 'Subscription successful'})
    except (KeyError, ValueError, TypeError) as e:
        _log.error(f"Invalid subscription request: {e}")
        emit('subscription_error', {'error': str(e)})
    except CommunicationError as e:
        _log.error(f"Communication error during subscription: {e}")
        emit('subscription_error', {'error': "Communication error with device"})
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Unexpected error during subscription: {e}")
        emit('subscription_error', {'error': "An unexpected error occurred"})

# on_unsubscribe
@socketio.on('unsubscribe_from_property')
@authenticated_only
async def on_unsubscribe(data):
    """Handles unsubscription requests from the WebSocket client."""

    _log.debug(f"Received unsubscribe request: {data}")

    try:
        device_id = int(data['deviceId'])
        object_type = data['objectType']
        object_instance = int(data['objectInstance'])
        property_identifier = data['propertyIdentifier']
        obj_id = (object_type, object_instance)

        # Unsubscribe and remove from subscriptions
        await app.unsubscribe_cov(device_id, obj_id, property_identifier)
        _log.info(f"Unsubscribed from {obj_id}.{property_identifier} on device {device_id}")

        emit('unsubscribe_success', {'message': 'Unsubscription successful'})
    except (KeyError, ValueError, TypeError) as e:
        _log.error(f"Invalid unsubscribe request: {e}")
        emit('unsubscribe_error', {'error': str(e)})
    except Exception as e:
        _log.exception(f"Unexpected error during unsubscribe: {e}")
        emit('unsubscribe_error', {'error': "An unexpected error occurred"})


# authenticate
@socketio.on('authenticate')
def authenticate(auth_data):
    """Handles authentication requests from WebSocket clients."""
    _log.debug(f"Received authentication request: {auth_data}")
    username = auth_data.get('username')
    password = auth_data.get('password')

    if verify_password(username, password):
        _log.info(f"User {username} authenticated successfully.")
        session[request.sid] = True  # Mark the session as authenticated
        join_room(request.sid)       # Join the user to their own room for personalized updates
        emit('authentication_success', {'message': 'Authentication successful'})
    else:
        _log.warning(f"Authentication failed for user: {username}")
        disconnect()  # Disconnect if authentication fails


# on_disconnect
@socketio.on('disconnect')
def on_disconnect():
    """Handles WebSocket disconnections."""
    _log.info(f"WebSocket client disconnected: {request.sid}")
    leave_room(request.sid)  # Remove the user from their room when they disconnect

    
# Flask API Endpoints

# get_devices
@app_flask.route('/devices')
@auth.login_required  # Require authentication for this endpoint
def get_devices():
    """API endpoint to get a list of discovered devices."""
    _log.debug("Fetching list of discovered devices")

    try:
        filter_name = request.args.get("name")
        sort_by = request.args.get("sort_by", "device_id")  # Default sorting by device ID

        # Get device information from DeviceInfoCache
        devices = [
            {
                "device_id": device_info.device_identifier[1],
                "device_name": device_info.device_name,
                "device_address": str(device_info.address),
            }
            for device_info in app.deviceInfoCache.get_device_infos()  # Iterate over DeviceInfo objects
            if (not filter_name or filter_name.lower() in device_info.device_name.lower())  # Apply filtering
        ]

        # Sort devices based on the 'sort_by' parameter
        if sort_by == "device_id":
            devices.sort(key=lambda x: x["device_id"])
        elif sort_by == "device_name":
            devices.sort(key=lambda x: x["device_name"])

        return jsonify(devices)  # Return the list of devices as JSON

    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Error getting devices: {e}")
        return jsonify({"error": "Internal Server Error"}), 500



# get_device_objects
@app_flask.route("/devices/<int:device_id>/objects")
@auth.login_required
async def get_device_objects(device_id):
    """API endpoint to get a list of objects for a specific device."""
    _log.debug(f"Fetching object list for device {device_id}")
    try:
        objects = await app.get_object_list(device_id)  
        if objects is not None:
            return jsonify(objects)
        else:
            return jsonify({"error": "Failed to retrieve object list"}), 500
    except (CommunicationError, TimeoutError, BACpypesError) as e:
        _log.error(f"Error getting objects for device {device_id}: {e}")
        return jsonify({"error": "Communication Error"}), 500
    except BACpypesError as e:
        _log.error(f"BACpypes error getting objects for device {device_id}: {e}")
        return jsonify({"error": "BACnet Communication Error"}), 500 



# get_object_properties
@app_flask.route('/objects/<object_type>/<int:object_instance>/properties')
@auth.login_required
async def get_object_properties(object_type, object_instance):
    """API endpoint to get a list of properties and their values for a specific object."""
    obj_id = (object_type, object_instance)

    _log.debug(f"Fetching properties for object {obj_id}")

    try:
        # Fetch device_id from query parameters or form data, raise ValueError if not provided
        device_id = int(request.args.get('device_id') or request.form.get('device_id'))
        _log.debug(f"Fetching device id from request parameters: device_id={device_id}")
        
        # Check if the device exists
        if device_id not in app.discovered_devices:
            raise ValueError(f"Device with ID {device_id} not found.")

        # Properties to read (customize as needed)
        properties_to_read = ["propertyList", "objectName", "description", "units", "presentValue", "statusFlags"]

        result = await app.property_reader.read_multiple_properties(
            device_id, obj_id, properties_to_read
        )
        if result is not None:
            property_values = {}
            for (_, prop_id), value in result.items():
                if isinstance(value, ArrayOf):
                    value = value[0]  # Extract single value if ArrayOf
                property_values[prop_id] = value
            return jsonify(property_values)
        else:
            _log.error(f"Error reading properties from device {device_id}: {result}")
            return jsonify({"error": "Failed to read properties"}), 500

    except (KeyError, ValueError, TypeError) as e:  # Catch specific errors
        _log.error(f"Invalid request or error processing properties: {e}")
        return jsonify({"error": str(e)}), 400  # Bad Request for invalid input
    except BACpypesError as e:
        _log.error(f"Communication error with device {device_id}: {e}")
        return jsonify({"error": "BACnet Communication Error"}), 500  # Internal Server Error

# handle_property
@app_flask.route('/objects/<object_type>/<int:object_instance>/properties/<property_name>', methods=['GET', 'PUT'])
@auth.login_required
async def handle_property(object_type, object_instance, property_name):
    """API endpoint to read/write a property value."""
    obj_id = (object_type, object_instance)
    try:
        # Fetch device_id from query parameters or form data, raise ValueError if not provided
        device_id = int(request.args.get('device_id') or request.form.get('device_id'))
        _log.debug(f"Fetching device id from request parameters: device_id={device_id}")

        # Check if the device exists
        if device_id not in app.discovered_devices:
            raise ValueError(f"Device with ID {device_id} not found.")

        if request.method == 'GET':
            value = await app.property_reader.read_property(device_id, obj_id, property_name)
            if value is not None:
                return jsonify({"value": value})
            else:
                return jsonify({"error": "Failed to read property"}), 500
        elif request.method == 'PUT':
            new_value = request.json.get('value')
            if new_value is None:
                return jsonify({"error": "Missing 'value' in request body"}), 400
            # Convert value to appropriate BACnet type if needed
            # ... (Add data type conversion logic here)
            await app.property_writer.write_property(device_id, obj_id, property_name, new_value)
            return jsonify({"message": "Property written successfully"})
    except (ValueError, KeyError) as e:
        return jsonify({"error": str(e)}), 400
    except BACpypesError as e:
        return jsonify({"error": "BACnet communication error"}), 500


@app_flask.route('/subscriptions', methods=['GET', 'POST', 'DELETE'])
@auth.login_required
async def handle_subscriptions():
    """Handles COV subscriptions: GET to list, POST to create, DELETE to remove."""
    _log.debug(f"Received {request.method} request for /subscriptions")

    try:
        if request.method == 'GET':
            subscriptions_data = [
                {
                    'device_id': sub.device_id,
                    'object_id': sub.obj_id,
                    'property_id': sub.prop_id,
                    'confirmed_notifications': sub.confirmed_notifications,
                    'lifetime_seconds': sub.lifetime_seconds
                }
                for sub in app.subscriptions.keys()
            ]
            return jsonify(subscriptions_data)

        elif request.method == 'POST':
            data = request.get_json()
            device_id = int(data.get('device_id'))
            obj_id = (data.get('object_type'), int(data.get('object_instance')))
            prop_id = data.get('property_identifier')
            confirmed_notifications = data.get('confirmed_notifications', True)  # Default to True
            lifetime_seconds = int(data.get('lifetime_seconds', 0)) if data.get('lifetime_seconds') is not None else None

            _log.info(f"Creating subscription: {obj_id}.{prop_id} on device {device_id}")

            # Input Validation
            if not all([device_id, obj_id, prop_id]):
                return jsonify({"error": "Missing required parameters."}), 400

            # Check if the device exists and is registered
            device_info = app.deviceInfoCache.get_device_info(device_id)
            if not device_info:
                return jsonify({"error": f"Device with ID {device_id} not found or not registered."}), 404
            if obj_id not in device_info.object_list:
                return jsonify({"error": f"Object {obj_id} not found in registered list for device {device_id}."}), 404
            if prop_id not in device_info.get_object_property(obj_id, "propertyList"):
                return jsonify({"error": f"Property {prop_id} not found in registered list for object {obj_id} on device {device_id}."}), 404

            # Create subscription and store it
            subscription = Subscription(device_id, obj_id, prop_id, confirmed_notifications, lifetime_seconds)
            app.subscriptions[subscription] = subscription
            await app.subscribe_cov(subscription)
            return jsonify({"message": "Subscription created successfully."}), 201

        elif request.method == 'DELETE':
            data = request.get_json()
            device_id = int(data.get('device_id'))
            obj_id = (data.get('object_type'), int(data.get('object_instance')))
            prop_id = data.get('property_identifier')

            # Input Validation
            if not all([device_id, obj_id, prop_id]):
                return jsonify({"error": "Missing required parameters."}), 400

            # Unsubscribe
            await app.unsubscribe_cov(device_id, obj_id, prop_id)
            return jsonify({"message": "Subscription deleted successfully."}), 200
    
    except (KeyError, ValueError, TypeError) as e:
        _log.error(f"Error handling subscriptions: {e}")
        return jsonify({"error": str(e)}), 400
    except BACpypesError as e:
        _log.error(f"BACpypes error handling subscriptions: {e}")
        return jsonify({"error": "BACnet communication error"}), 500


@app_flask.route('/alarms')
@auth.login_required
def get_alarms():
    """API endpoint to get a list of all active and acknowledged alarms."""
    _log.debug("Fetching list of active and acknowledged alarms.")
    try:
        active_alarms = [
            {
                'device_id': alarm_key[0],
                'object_id': alarm_key[1],
                'property_id': alarm_key[2],
                'alarm_type': alarm_key[3],
                'alarm_value': alarm_data['alarm_value'],
                'timestamp': alarm_data['timestamp'],
                'severity': alarm_data['severity'],
            }
            for alarm_key, alarm_data in app.alarm_manager.active_alarms.items()
        ]
        acknowledged_alarms = [
            {
                'device_id': alarm_key[0],
                'object_id': alarm_key[1],
                'property_id': alarm_key[2],
                'alarm_type': alarm_key[3],
                'timestamp': alarm_data['timestamp'],
                'severity': alarm_data['severity'],
            }
            for alarm_key, alarm_data in app.alarm_manager.acknowledged_alarms.items()
        ]
        all_alarms = active_alarms + acknowledged_alarms
        return jsonify(all_alarms)

    except Exception as e:  # Catch-all for unexpected errors
        _log.exception(f"Error getting alarms: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

    
@app_flask.route('/alarms', methods=['PUT'])
@auth.login_required
async def handle_alarms():
    """API endpoint to acknowledge an alarm."""
    _log.debug("Handling alarm acknowledgement request")
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
    except (ValueError, KeyError, TypeError) as e:
        _log.error(f"Error acknowledging alarm: {e}")
        return jsonify({"error": "Invalid request data"}), 400
    except BACpypesError as e:
        _log.error(f"BACpypes error acknowledging alarm: {e}")
        return jsonify({"error": "BACnet Communication Error"}), 500
    
    
# acknowledge_alarm (POST)
@app_flask.route('/alarms/acknowledge', methods=['POST'])
@auth.login_required
async def acknowledge_alarm_post():
    """API endpoint to acknowledge an alarm (alternative route)."""
    return await handle_alarms()  

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

# silence_alarm
@app_flask.route('/alarms/silence', methods=['POST'])
@auth.login_required
def silence_alarm():
    """API endpoint to silence an alarm."""
    _log.debug("Received request to silence alarm.")

    try:
        data = request.get_json()
        device_id = int(data.get('device_id'))
        object_id = tuple(data.get('object_id'))
        property_id = data.get('property_id')
        alarm_type = data.get('alarm_type')
        duration = int(data.get('duration', 300))  # Default silence duration of 5 minutes (300 seconds)

        if not all([device_id, object_id, property_id, alarm_type]):
            return jsonify({"error": "Missing required parameters."}), 400

        alarm_key = (device_id, object_id, property_id, alarm_type)

        # Verify if the alarm is active
        if alarm_key not in app.alarm_manager.active_alarms:
            return jsonify({"error": "Alarm not found or not active."}), 404

        # Silence the alarm
        app.alarm_manager.silence_alarm(device_id, object_id, property_id, alarm_type, duration)
        return jsonify({"message": "Alarm silenced successfully."}), 200

    except (ValueError, KeyError, TypeError) as e:
        _log.error(f"Error silencing alarm: {e}")
        return jsonify({"error": str(e)}), 400



# get_alarm_history
@app_flask.route('/alarms/history')
@auth.login_required
async def get_alarm_history():
    """API endpoint to get alarm history."""
    _log.debug("Fetching alarm history from database")
    try:
        query = "SELECT * FROM alarms ORDER BY timestamp DESC"  # Retrieve all alarms in descending order by timestamp
        rows = fetch_data("bacee.db", query)  # Fetch data from the database
        alarms = []
        for row in rows:
            alarm = {
                'id': row[0],
                'timestamp': row[1],
                'device_id': row[2],
                'object_id': row[3],
                'property_id': row[4],
                'alarm_type': row[5],
                'alarm_value': row[6],
                'z_score': row[7],
                'is_anomaly': bool(row[8]),
                'acknowledged': bool(row[9]),
                'cleared': bool(row[10]),
            }
            alarms.append(alarm)
        return jsonify(alarms), 200  # Return the alarms as JSON with a 200 OK status

    except sqlite3.Error as e:
        _log.error(f"Error retrieving alarm history from database: {e}")
        return jsonify({"error": "Database Error"}), 500

def start_api_server():
    app_flask.run(host="0.0.0.0", port=5000)  # Start on all interfaces, port 5000


# Configuration Validation Function
def validate_configurations(configurations, validation_rules):
    """Validates configurations using a dictionary of rules."""
    _log.debug(f"Validating configurations: {configurations}")

    for key, rule in validation_rules.items():
        value = configurations.get(key, "")
        if not rule(value):
            _log.error(f"Invalid configuration: {key}={value}")
            return False
    return True

# JSON Validation Function
def validate_json_file(file_path, schema_file_path=None):
    """Validates a JSON file against a schema (if provided)."""
    _log.debug(f"Validating JSON file: {file_path}")

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        if schema_file_path:
            with open(schema_file_path, "r") as schema_file:
                schema = json.load(schema_file)
            validate(instance=data, schema=schema)  # Schema validation
        return True
    except (json.JSONDecodeError, FileNotFoundError, jsonschema.ValidationError) as e:
        _log.error(f"JSON validation failed: {e}")
        return False

# Database Validation Function
def validate_database(db_path):
    """Validates database connection and schema."""
    _log.debug(f"Validating database: {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            # Execute schema validation queries here (e.g., checking table existence)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alarms'")
            if not cursor.fetchone():  # Check if 'alarms' table exists
                _log.error("Table 'alarms' not found in the database.")
                return False
        return True
    except sqlite3.Error as e:
        _log.error(f"Database validation failed: {e}")
        return False

def start_api_server():
    """Starts the Flask API server."""
    socketio.run(app_flask, host="0.0.0.0", port=5000) # Use socketio.run to start the server since we are using web sockets
        
# Main function
async def main():

    _log.debug("Starting main application function...")
    
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

    if not validate_configurations(configurations, validation_rules):
        _log.critical("Configuration validation failed. Exiting.")
        return  
        
    # **************************************************************************
        
    # BACnet Application Setup (with enhanced async task handling)
    tasks = set()  # Set to keep track of running tasks
    try:
        _log.info("Starting BACnet application...")
        app = BACeeApp(LOCAL_ADDRESS, DEVICE_ID, DEVICE_NAME)

        # Create task for BACnet start and BBMD registration
        bacnet_task = asyncio.create_task(app.start())  # Don't need the inner function here
        tasks = {bacnet_task}  # Initialize tasks set

        # Create other tasks (manage_subscriptions, manage_alarms, cli_loop)
        tasks.update({
            asyncio.create_task(coro)
            for coro in [app.manage_subscriptions(), app.alarm_manager.manage_alarms(), cli_loop(app)]
        })

        # Start API server task
        api_server_task = asyncio.create_task(start_api_server())  # Assuming start_api_server is defined
        tasks.add(api_server_task)

        # Wait for any task to complete
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Handle the finished task
        for done_task in done:
            try:
                await done_task
            except asyncio.CancelledError:
                pass  # Ignore CancelledError (expected during shutdown)
            except Exception as e:
                _log.exception("An error occurred in a task:", exc_info=e)

        # Cancel remaining tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # Ignore CancelledError
    except Exception as e:  # Catch-all for unexpected errors
        _log.exception("An error occurred in the main function:", exc_info=e)


if __name__ == '__main__':
    asyncio.run(main())


