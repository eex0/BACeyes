# BACee

# Modular BACnet Communication for BBMD Devices for mid to lrg networks: uses bacpypes
# this code provides a framework for BACnet communication and COV subscription management. 
# It allows you to discover BBMDs, monitor BACnet objects for changes, and receive 
# notifications when property values change...

# MIT License - 2024 by: eex0

# Thank You: 
#            * Joel Bender for bacpypes

"""

Here's a breakdown of the key functionalities:

    BBMD Discovery (BBMDManager class):

        Discovers BACnet Broadcast and Device Management (BBMD) servers on the network.
        Uses a WHO-IS message to find available BBMDs.
        Parses the response to extract BBMD addresses.

    COV Monitoring (COVMonitor class):
        Monitors a specific BACnet object for changes in its properties.
        Takes an object reference and a BACnet manager as input.
        Defines a set_monitored_properties method to specify which properties to track.
        Implements an execute method to read current property values and compare them with previous values.
        If a change is detected, it triggers a notification with the new value.

    COV Management (COVManager class):
        Handles COV subscriptions for monitoring BACnet object properties.
        Maintains a dictionary of subscriptions with object references as keys.
        Processes incoming COV requests (subscriptions, notifications) and responses.
        Provides methods for subscribing to changes, handling notifications, and error handling.
        Uses different COV monitor implementations based on object type or properties.

    Subscription Management (COVSubscription class):
        Represents a COV subscription for a specific object and property.
        Stores details like object reference, property ID, callback function (optional), and subscription duration.
        Includes methods for canceling subscriptions and handling renewals.

    COV Detection Algorithms (COVDetectionAlgorithm class and subclasses):
        Defines a base class for implementing COV detection algorithms.
        Subclasses can be created for specific detection logic (e.g., AnalogValueCOVDetection).
        The execute method performs the actual detection based on property values.

    BACnet Client (BACnetClient class):
        Represents a BACnet client that interacts with BACnet devices on the network.
        Initializes a BACnet network object and a COV manager.
        Provides methods for discovering devices, reading property values, and subscribing to changes.

Overall, this code provides a framework for BACnet communication with a focus on monitoring and detecting changes in BACnet object properties. 
It allows for flexible configuration of COV subscriptions and implementing custom detection algorithms.

"""

import bacpypes
import json
import logging
import socket
import threading
import time

from bacpypes.object import validateObjectId
from bacpypes.errors import CommunicationError, BACnetError
from threading import Lock

class BBMDManager(BACnetManager):
    def __init__(self):
        super().__init__()
        self.bdt_table = None
        self.discover_bbmds()

    def discover_bbmds(self):
        self.bdt_table = {}
        who_is_message = self.create_who_is_message()
        for network in range(1, 256):
            for port in [47808]:  # Corrected port number for BACnet
                try:
                    response = self.send_message(network, port, who_is_message)
                    if response and self.is_iam_response(response):
                        address = self.extract_bbmd_address(response)
                        self.bdt_table[address] = {"network": network, "port": port}
                except Exception as e:
                    logging.error(f"Error discovering BBMD on network {network}, port {port}: {e}")
        self.bdt_communicator = BBMDCommunicator(self.bdt_table)
        return

    def create_who_is_message(self):
        message = Message()
        pdu = PDU(
            pdu_type=PDU.PDU_TYPE_CONFIRMED_REQUEST,
            service_id=ServiceOption.SERVICE_CONFIRMED_WHO_IS,
        )
        pdu.destination_object_type = BacnetObjectType.OBJECT_DEVICE
        message.pdu = pdu
        return message

    def is_iam_response(self, response):
        service_id = get_service_id(response)
        return service_id == 0

    def extract_bbmd_address(self, response_data):
        try:
            decoded_data = decode_asn1(response_data)
            bbmd_address = decoded_data["WhoIs"]["BACnetAddress"]
            return bbmd_address
        except Exception as e:
            logging.error(f"Error parsing BBMD address: {e}")
            return None

    def get_bdt_table(self):
        if self.bdt_table:
            return self.bdt_table.copy()
        return None

    def send_broadcast_message(self):
        try:
            self.bbmd_communicator.send_broadcast(message)
        except CommunicationError as e:
            logging.error(f"Error sending broadcast message: {message} - {e}")
            raise

        
class COVMonitor:
    def __init__(self, object_reference, bacnet_manager):
        self.object_reference = object_reference
        self.bacnet_manager = bacnet_manager
        self.monitored_properties = []
        self.previous_values = {}

    def set_monitored_properties(self, properties):
        self.monitored_properties = properties

    def execute(self):
        for prop_id in self.monitored_properties:
            current_value = self.bacnet_manager.read_property(self.object_reference, prop_id)
            previous_value = self.previous_values.get(prop_id)
            if previous_value is None or abs(current_value - previous_value) > 0.01:
                self.previous_values[prop_id] = current_value
                self.notify_change(prop_id, current_value)
                return True
        return False

    def notify_change(self, prop_id, value):
        logging.info(f"Change detected: object {self.object_reference}, property {prop_id}, new value: {value}")

    def get_notification_data(self, prop_id):
        return self.previous_values.get(prop_id)


def discover_devices(self) -> list[str]:
    devices = []
    try:
        if BBMDManager:
            bbmd_manager = BBMDManager()
            try:
                bdt_table = bbmd_manager.get_bdt_table()
                if bdt_table:
                    for address, _ in bdt_table.items():
                        devices.append(address)
            except Exception as e:  # Catch errors during BBMD communication
                self.logger.warning(f"Error communicating with BBMD: {e}")
    except ImportError:
        self.logger.warning("BBMDManager not found. Device discovery")

class COVManager:
    def __init__(self, primary=True, backup_address=None):
        self.primary = primary
        self.backup_address = backup_address
        self.subscriptions = {}
        self.lock = threading.Lock()
        self.heartbeat_timer = None
        self.socket = None
        self.failover_timer = None
        self.backup_server_address = "..."  
        self.renewal_lock = threading.Lock()  
        
    def do_SubscribeCOVRequest(self, apdu):
        obj_ref = apdu.decode_object_reference()
        if obj_ref not in self.device.objects:
            self.cov_reject(None, apdu, None)
            return

        cov_monitor = self._get_cov_monitor(obj_ref)
        subscription = COVSubscription(obj_ref, client_addr, proc_id, obj_id, confirmed, lifetime)
        subscription.cov_monitor = cov_monitor
        self.add_subscription(subscription)
        self.cov_ack(subscription, apdu, None)
        if self.primary:
            self.send_message({"type": "subscription", "data": subscription.to_dict()}, self.backup_address)

    def add_subscription(self, subscription):
        if subscription.object_ref not in self.subscriptions:
            self.subscriptions[subscription.object_ref] = []
        self.subscriptions[subscription.object_ref].append((subscription, subscription.cov_monitor))

    def _validate_subscription(self, object_ref, prop_id, params):
      # Implement logic to validate object, property, and subscription parameters
      return True  # Replace with actual validation logic

    def cancel_subscription(self, subscription):
        subscriptions = self.subscriptions.get(subscription.object_ref)
        if subscriptions:
            for i, (cov_sub, _) in enumerate(subscriptions):
                if cov_sub == subscription:
                    del subscriptions[i]
                    break
            if not subscriptions:
                del self.subscriptions[subscription.object_ref]

    def cov_notification(self, subscription, request):
        pass

    def handle_cov_notification(self, apdu):
        if self.confirmed:
            ack_pdu = SimpleAckPDU(context=apdu)
            self.send_ack(ack_pdu)

    def do_ConfirmedCOVNotificationRequest(self, apdu):
        cov_monitor = self.find_cov_monitor(apdu.monitoredObjectIdentifier)
        if cov_monitor:
            cov_monitor.handle_confirmed_cov_notification(apdu)
        else:
            self.handleError(f"COV monitor not found for object ID: {apdu.monitoredObjectIdentifier}")

        try:
            new_value = self._extract_value_from_apdu(apdu)
        except Exception as e:
            self.handleError(f"Error extracting value from COV notification: {e}")
            return

        if cov_monitor:
            cov_monitor.handle_new_value(new_value)

    def do_UnconfirmedCOVNotificationRequest(self, apdu):
        cov_monitor = self.find_cov_monitor(apdu.monitoredObjectIdentifier)
        if cov_monitor:
            cov_monitor.handle_unconfirmed_cov_notification(apdu)

    def cov_confirmation(self, iocb):
        pass

    def cov_ack(self, subscription, request, response):
        pass

    def send_ack(self, ack_pdu):
        try:
            self.bacnet_manager.send_pdu(ack_pdu)
        except Exception as e:
            self.handleError(f"Failed to send COV ack: {e}")

    def handleError(self, message):
        logging.error("COVManager Error: %s", message)

    def cov_error(self, subscription, request, response):
        """Handles errors received during COV communication."""
        # Handle specific BACnet errors (e.g., BACnetRejectRequest, BACnetAbort)
        if isinstance(response, BACnetRejectRequest):
            self.handleError(f"COV subscription rejected: {response.reason}")
        elif isinstance(response, BACnetAbort):
            self.handleError(f"COV subscription aborted: {response.reason}")
        # ... (handle other errors)

    def _get_cov_monitor(self, object_reference):
        if object_reference.object_type == BacnetObjectType.OBJECT_ANALOG_VALUE:
            return AnalogValueCOVMonitor(object_reference, self.bacnet_manager)

    def cov_reject(self, subscription, request, response):
        pass

    def cov_abort(self, subscription, request, response):
        pass

    def _extract_value_from_apdu(self, apdu):
        obj_type = apdu.monitoredObjectIdentifier.object_type
        prop_id = apdu.decode_property_id()

        if obj_type == BacnetObjectType.OBJECT_ANALOG_VALUE:
            if prop_id == PropertyIdentifier.PRESENT_VALUE:
                return apdu.value.cast_to(float)
        elif obj_type == BacnetObjectType.OBJECT_BINARY_OUTPUT:
            if prop_id == PropertyIdentifier.PRESENT_VALUE:
                return apdu.value.cast_to(bool)
        elif obj_type == BacnetObjectType.OBJECT_MULTISTATE_OUTPUT:
            if prop_id == PropertyIdentifier.PRESENT_VALUE:
                return apdu.value.cast_to(int)
                
        # ... (handle other BACnetObjectType as needed)

        else:
            raise NotImplementedError(f"Value extraction for object type {obj_type} not implemented")

    def _get_detection_algorithm(self, obj_ref):
        if isinstance(self.device.objects[obj_ref], AnalogValueObject):
            return AnalogValueCOVMonitor(obj_ref, self.bacnet_manager)
        else:
            return BasicCOVMonitor(obj_ref, self.bacnet_manager)

    def start(self):
        if self.primary:
            self.start_heartbeat()
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.backup_address, PORT))
            self.listen_for_updates()

    def start_heartbeat(self):
        self.heartbeat_timer = threading.Timer(5, self.send_heartbeat)
        self.heartbeat_timer.start()

    def send_heartbeat(self):
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                if self.primary and self.socket:
                    self.send_message({"type": "heartbeat"}, self.backup_address)
                    break
            except Exception as e:
                logging.error(f"Error sending heartbeat (attempt {retry_count+1}/{max_retries}): {e}")
                if self.primary:
                    try:
                        self.socket.close()
                    except OSError:
                        logging.info("Socket already closed")
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                retry_count += 1
                time.sleep(2)

        if retry_count == max_retries:
            logging.error("Failed to send heartbeat after retries.")

        self.heartbeat_timer = threading.Timer(5, self.send_heartbeat)
        self.heartbeat_timer.start()
  
    def listen_for_updates(self):
        while True:
            try:
                with self.renewal_lock:
                    data = self.socket.recv(1024)
                    if data:
                        logging.info("Received update data from primary server")
            except Exception as e:
                logging.error(f"Error receiving update data: {e}")
                break
        self.socket.close()

    def send_message(self, message, address):
        try:
            self.socket.connect(address) if not self.primary else None
            self.socket.sendall(json.dumps(message).encode())
        except Exception as e:
            print(f"Error sending message: {e}")
        finally:
            self.socket.close() if not self.primary else None

    def process_update(self, update):
        if update["type"] == "subscription":
            self.handle_subscription(update["data"])
        elif update["type"] == "property_change":
            self.handle_property_change(update["data"])
        elif update["type"] == "heartbeat":
            self.reset_failover_timer()
        else:
            print(f"Unknown update type: {update['type']}")

    def subscribe_to_changes(self, object_reference, property_id, callback, duration=None):
      # Existing subscription logic (validation, COV monitor selection)
      subscription = COVSubscription(object_reference, property_id, callback, duration)
      self.subscriptions[object_reference] = (subscription, cov_monitor)
      if duration is not None:
        self.schedule_renewal(subscription)
      return subscription

    def schedule_renewal(self, subscription):
      with self.renewal_lock:  # Acquire lock for thread safety
        if subscription.duration is not None and not subscription.renewal_scheduled:
          # Schedule renewal task using timer
          timer = threading.Timer(subscription.duration, self._handle_renewal, args=[subscription])
          timer.start()
          subscription.renewal_scheduled = True

    def handle_subscription(self, subscription_data):
      object_ref = subscription_data.get("objectRef")
      prop_id = subscription_data.get("propertyId")
      params = subscription_data.get("params", {})  # Optional parameters

      # Validate subscription and create/update COV monitor
      if self._validate_subscription(object_ref, prop_id, params):
        monitor = self._get_cov_monitor(object_ref)  # Get or create monitor
        # ... (set monitor properties based on subscription data and params)
        self.subscriptions[object_ref][prop_id] = (monitor, params)  # Store subscription

    def handle_property_change(self, property_data):
      object_ref = property_data.get("objectRef")
      prop_id = property_data.get("propertyId")
      new_value = property_data.get("value")

      subscriptions = self.subscriptions.get(object_ref)
      if subscriptions:
        for _, monitor in subscriptions.values():
          if monitor.monitored_properties and prop_id in monitor.monitored_properties:
            monitor.handle_new_value(new_value)

    def _handle_renewal(self, subscription):
      with self.renewal_lock:  # Acquire lock for thread safety
        if subscription in self.subscriptions:  # Check if subscription still exists
          self.subscriptions[subscription.object_reference] = self.renew_subscription(subscription)

    def renew_subscription(self, subscription):
      # Re-subscribe logic (call subscribe_to_changes with existing parameters)
      return self.subscribe_to_changes(subscription.object_reference, subscription.property_id, subscription.callback, subscription.duration)

    def reset_failover_timer(self):
      """Resets the timer for tracking backup server health."""
      if self.failover_timer:
         self.failover_timer.cancel()  # Cancel previous timer if running
         self.failover_timer = Timer(self.failover_timeout, self._handle_failover)
         self.failover_timer.start()
        
    def _handle_failover(self):
      """Initiate failover procedures if backup server is unavailable."""
      # Implement logic to transfer subscriptions or data to backup server
      #  using the configured backup_server_address
      print("Failover initiated! Transferring data to backup server...")


class COVSubscription:
    def __init__(self, object_reference, property_id, callback, duration=None):
      self.object_reference = object_reference
      self.property_id = property_id
      self.callback = callback
      self.duration = duration  # New field for subscription duration
      self.renewal_scheduled = False  # Flag to track renewal scheduling

    def to_dict(self):
        return {
            "object_ref": self.object_ref.to_dict(),
            "client_addr": self.client_addr,
            "proc_id": self.proc_id,
            "obj_id": self.obj_id,
            "confirmed": self.confirmed,
            "lifetime": self.lifetime,
        }

    def cancel_subscription(self, cov_manager):
      # Existing cancellation logic (remove from COVManager dictionary)
      cov_manager.remove_subscription(self)

    def renew_subscription(self, cov_manager):
      if self.duration is not None and not self.renewal_scheduled:
        # Schedule renewal task (can be implemented in different ways)
        # Here's an example using a timer:
        import threading
        timer = threading.Timer(self.duration, self._handle_renewal, args=[cov_manager])
        timer.start()
        self.renewal_scheduled = True

    def process_task(self):
        self.cancel_subscription()

    def _handle_renewal(self, cov_manager):
      # Re-subscribe with COVManager
      cov_manager.subscribe_to_changes(self.object_reference, self.property_id, self.callback, self.duration)
      self.renewal_scheduled = False  # Reset renewal flag

class COVDetectionAlgorithm:
    def __init__(self, monitored_properties, reported_properties):
        self.monitored_properties = monitored_properties
        self.reported_properties = reported_properties

    def execute(self):
        pass

    def send_cov_notifications(self):
        subscriptions = self.cov_manager.subscriptions.get(self.object_ref)
        if subscriptions:
            for subscription, _ in subscriptions:
                pass

class BasicCOVDetection(COVDetectionAlgorithm):
    def __init__(self):
        super().__init__(["presentValue", "statusFlags"], ["presentValue", "statusFlags"])
    def execute(self):
        pass
        
class AnalogValueCOVDetection(COVDetectionAlgorithm):
    def __init__(self, object_reference, bacnet_manager):
        super().__init__(object_reference, bacnet_manager)
        self.threshold = 0.1  # Default threshold for change detection

    def execute(self):
        obj_id = self.object_id
        prop_id = self.property_ids[0]
        current_value = bacnet_manager.read_property(object_id=obj_id, property_id=prop_id)
        previous_value = self.get_previous_value(obj_id, prop_id)
        difference = abs(current_value - previous_value)
        if difference > self.delta:
            self.send_cov_notifications(obj_id, prop_id, current_value)
            self.store_previous_value(obj_id, prop_id, current_value)

    def send_cov_notifications(self, obj_id, prop_id, value):
        print(f"COV detected: object {obj_id}, property {prop_id}, new value: {value}")

    def store_previous_value(self, obj_id, prop_id, value):
        pass

    def get_previous_value(self, obj_id, prop_id):
        pass

    def handle_new_value(self, value):
        # Implement logic for threshold detection or rate of change monitoring
        previous_value = self.get_notification_data()
        if abs(value - previous_value) > self.threshold:
            # Change detected, trigger custom action (e.g., notification)
            print(f"Significant change detected for {self.object_reference}: {previous_value} -> {value}")

    def get_notification_data(self):
        return self.bacnet_manager.cov_manager.get_previous_value(self.object_reference)

def subscribe_to_changes(object_reference, property_id, bacnet_manager, callback=None, monitor_type=None):
    if not bacnet_manager.cov_manager:
        print("COV manager not available for subscriptions")
        return None

    # Implement logic to create a COV subscription message based on monitor_type
    subscription = COVSubscription(
        object_reference,
        bacnet_manager.get_client_address(),
        0,  # Process ID (can be incremented for multiple subscriptions)
        property_id,
        confirmed=True,  # Use confirmed subscriptions for reliable change detection
        lifetime=255,  # Set a subscription lifetime (e.g., 255 seconds)
    )

    # Choose COV monitor based on monitor_type or object type
    if monitor_type:
        cov_monitor = monitor_type(object_reference, bacnet_manager)
    else:
        cov_monitor = bacnet_manager.cov_manager._get_cov_monitor(object_reference)
    subscription.cov_monitor = cov_monitor

    try:
        bacnet_manager.cov_manager.subscribe(subscription)
        if callback:
            # Define a separate notification handling function
            def handle_notification(value):
                callback(object_reference, property_id, value)
            cov_monitor.handle_new_value = handle_notification
        return subscription
    except Exception as e:
        print(f"Error subscribing to changes: {e}")
        return None

class BACnetClient:
    def __init__(self, address: str):
        self.address = address
        self.network = bacpypes.network.Network()
        self.cov_manager = COVManager(self.network)
        self.bbmd_discoverer = None        
        self.logger = logging.getLogger(__name__)        

    def discover_devices(self) -> list[str]:
        devices = []
        try:
            if BBMDManager:
                bbmd_manager = BBMDManager()
                bdt_table = bbmd_manager.get_bdt_table()
                if bdt_table:
                    for address, _ in bdt_table.items():
                        devices.append(address)
        except ImportError:
            self.logger.warning("BBMDManager not found. Device discovery skipped.")
        return devices

    def read_property(self, object_identifier: BACnetObjectIdentifier, property_id: int) -> Optional[Any]:

      # Validate input
      if not validateObjectId(object_identifier):
        raise ValueError("Invalid object identifier provided")

      try:
        # Use network.send_pdu to send the request
        response = self.network.send_pdu(
            ReadPropertyRequest(
                objectIdentifier=object_identifier,
                propertyIdentifier=property_id,
            )
        )

        # Check for valid response and error codes
        if not response or response.pdu.serviceChoice != ServiceChoice.READ_PROPERTY_RESPONSE:
          raise BACnetError(f"Error reading property: {object_identifier}:{property_id}")
        elif response.pdu.errorCode != bacpypes.StatusCode.DEVICE_STATUS_ERROR:
          raise BACnetError(f"BACnet error: {response.pdu.errorCode.name}")

        # Return the property value
        return response.pdu.valueList[0].value

      except (CommunicationError, BACnetError) as e:
        # Handle errors, log them, or raise for further handling
        self.logger.error(f"Error reading property: {e}")
        return None

      except Exception as e:
        # Catch unexpected exceptions and log them
        self.logger.error(f"Unexpected error while reading property: {e}")
        return None
    
    def write_property(self, object_identifier: BACnetObjectIdentifier, property_id: int, value: Any):

      # Validate input
      if not validateObjectId(object_identifier):
        raise ValueError("Invalid object identifier provided")

      # Check if value type matches property type (optional)
      # Implement logic to validate value based on property ID (e.g., using BACnet library)

      try:
        # Use network.send_pdu to send the request
        self.network.send_pdu(
            WritePropertyRequest(
                objectIdentifier=object_identifier,
                propertyIdentifier=property_id,
                valueList=[bacpypes.cast(value)],  # Cast value to appropriate BACnet type
            )
        )

      except (CommunicationError, BACnetError) as e:
        # Handle communication and BACnet errors
        raise BACnetError(f"Error writing property: {object_identifier}:{property_id} - {e}") from e

      except Exception as e:
        # Catch unexpected exceptions and log them
        self.logger.error(f"Unexpected error while writing property: {e}")
    
    def subscribe_to_changes(self, object_identifier: BACnetObjectIdentifier, property_id: int, callback=None):
        if self.cov_manager:
            self.cov_manager.subscribe(object_identifier, property_id, callback)
        else:
            self.logger.warning("COVManager not initialized. Subscription failed.")
    
    def handle_received_data(self, data: bytes):
        if data and data[0] == 0x00:
            self.logger.debug("Received UnconfirmedCOVNotification")
            self.cov_manager.process_unconfirmed_cov(data)
    
    def process_cov_notification(self, apdu):
        """Processes a COV notification APDU and extracts the new value."""
        if not apdu:
            return
        
        object_reference = apdu.decode_object_reference()
        property_id = apdu.decode_property_id()

        # Extract new value based on property type
        if apdu.get_context_specific(0).datatype == bacpypes.basic.UnsignedInteger:
            new_value = apdu.decode_context_specific_unsigned(0)
        elif apdu.get_context_specific(0).datatype == bacpypes.basic.Boolean:
            new_value = apdu.decode_context_specific_boolean(0)
        # ... (handle other data types as needed)
        else:
            self.logger.warning(f"Unsupported data type for COV notification: {apdu.get_context_specific(0).datatype}")
            return

        print(f"Received COV notification for object: {object_reference}")
        print(f"Property ID: {property_id}, New Value: {new_value}")

def main():
  """
  Main function.
  """

  # JSON filename (replace with your desired filename)
  filename = "bbmds_and_discovered_devices.json"

  # Container for BBMD data with discovered devices
  bbmd_data = {
      "bbmds": []
  }

  # Open the JSON file for read and update
  try:
    with open(filename, "r+") as f:
      try:
        # Attempt to load existing data (if any)
        bbmd_data = json.load(f)
      except json.JSONDecodeError:
        # Ignore loading errors if the file is empty
        pass

      # Loop through BBMD entries in the JSON structure (or an empty list)
      for bbmd_entry in bbmd_data["bbmds"]:
        address = bbmd_entry.get("address")
        port = bbmd_entry.get("port", 47848)  # Use default port if not specified

        # Initialize BACnet client with BBMD address
        bacnet_client = BACnetClient(address)

        # Discover devices using the BACnet client
        devices = bacnet_client.discover_devices()

        # Update discovered devices for this BBMD entry
        bbmd_entry["discovered_devices"] = [{"address": device} for device in devices]

      # Write the entire JSON data (including BBMDs and discovered devices)
      f.seek(0)  # Move to the beginning of the file
      json.dump(bbmd_data, f, indent=4)
      print(f"Discovered devices written to {filename}")
  except Exception as e:
    print(f"Error writing discovered devices to JSON: {e}")

if __name__ == "__main__":
  main()
        
