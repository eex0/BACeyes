import logging
from datetime import datetime
from typing import Callable

import aiosmtplib
from bacpypes3.apdu import ConfirmedEventNotificationRequest
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.basetypes import EventType, EventState, ServicesSupported
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import ssl
from BACeyesApp import BACeyesApp

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)

class BACeyesAlarmEvent:
    """Represents a BACnet alarm event."""

    def __init__(
        self,
        device_id: ObjectIdentifier,
        obj_id: ObjectIdentifier,
        event_type: EventType,
        event_state: EventState,
        priority: int,
        timestamp: datetime = datetime.now(),
    ):
        """
        Initializes an AlarmEvent object.

        Args:
            device_id: The ObjectIdentifier of the device generating the alarm.
            obj_id: The ObjectIdentifier of the object associated with the alarm.
            event_type: The type of the event (e.g., lifeSafetyAlarm).
            event_state: The current state of the event (e.g., normal, active, unconfirmed).
            priority: The priority level of the alarm.
            timestamp: The time the alarm event was generated.
        """

        self.device_id = device_id
        self.obj_id = obj_id
        self.event_type = event_type
        self.event_state = event_state
        self.priority = priority
        self.timestamp = timestamp
        self.acknowledged = False

    @classmethod
    def from_pdu(cls, pdu: ConfirmedEventNotificationRequest):
        """Creates an AlarmEvent from a ConfirmedEventNotificationRequest PDU."""
        return cls(
            pdu.initiatingDeviceIdentifier,
            pdu.eventObjectIdentifier,
            pdu.eventType,
            pdu.eventState,
            pdu.priority,
            pdu.eventTime,
        )

    def to_dict(self):
        """Converts the AlarmEvent to a dictionary."""
        return {
            "device_id": str(self.device_id),
            "obj_id": str(self.obj_id),
            "event_type": str(self.event_type),
            "event_state": str(self.event_state),
            "priority": self.priority,
            "timestamp": self.timestamp.isoformat(),
            "acknowledged": self.acknowledged,
        }


class BACeyesAlarmManager:
    """Manages BACnet alarm events."""

    def __init__(
            self,
            bacnet_app: "BACeyesApp",
            sender_email: str = None,
            receiver_email: str = None,
            password: str = None,
    ):
        """
        Initializes the AlarmManager.

        Args:
            bacnet_app: The BACnetApplication instance to use for communication.
            sender_email: The email address to send alarm notifications from (optional).
            receiver_email: The email address to send alarm notifications to (optional).
            password: The password for the sender email account (optional).
        """

        self.bacnet_app = bacnet_app
        self.active_alarms = {}
        self.alarm_handlers = []
        self.sender_email = sender_email
        self.receiver_email = receiver_email
        self.password = password

    def register_alarm_handler(self, handler: Callable[[BACeyesAlarmEvent], None]):
        """Registers a callback function to handle alarm events."""
        self.alarm_handlers.append(handler)

    async def process_event_notification(self, pdu: ConfirmedEventNotificationRequest):
        """Processes an incoming event notification."""

        # Get services supported by the device
        services_supported = self.bacnet_app.this_device.protocolServicesSupported

        # Check if the device supports alarms
        if not (services_supported & ServicesSupported.confirmedCOVNotification.value):
            _log.error(
                f"Device {pdu.initiatingDeviceIdentifier} does not support COV notifications."
            )
            return

        # Check if the notification class is for an alarm
        if pdu.notificationClass != 255:
            _log.info(f"Non-alarm event notification received from {pdu.initiatingDeviceIdentifier}")
            return

        alarm_event = BACeyesAlarmEvent.from_pdu(pdu)
        key = (alarm_event.device_id, alarm_event.obj_id)

        # Duplicate alarm handling
        if key in self.active_alarms and self.active_alarms[key].priority == alarm_event.priority:
            _log.debug(f"Duplicate alarm event ignored: {alarm_event.device_id} - {alarm_event.obj_id}")
            return

        self.active_alarms[key] = alarm_event
        _log.info(f"New alarm event: {alarm_event}")  # Log the entire alarm event object

        # Notify handlers and potentially send email (if configured)
        for handler in self.alarm_handlers:
            handler(alarm_event)

        if self.sender_email and self.receiver_email and self.password:
            await self.send_alarm_email(alarm_event)

    async def send_alarm_email(self, alarm_event: BACeyesAlarmEvent):
        """Sends an email notification for an alarm."""
        message = MIMEMultipart("alternative")
        message["Subject"] = f"BACnet Alarm: {alarm_event.device_id} - {alarm_event.obj_id}"
        message["From"] = self.sender_email
        message["To"] = self.receiver_email

        text = f"""\
        BACnet Alarm Notification
        Device: {alarm_event.device_id}
        Object: {alarm_event.obj_id}
        Time: {alarm_event.timestamp}
        Type: {alarm_event.event_type}  
        State: {alarm_event.event_state}
        Priority: {alarm_event.priority}
        """
        part1 = MIMEText(text, "plain")
        message.attach(part1)

        # Create a secure connection with server and send email
        context = ssl.create_default_context()
        try:
            async with aiosmtplib.SMTP(hostname="smtp.gmail.com", port=465, use_tls=True,
                                       tls_context=context) as server:
                await server.login(self.sender_email, self.password)
                await server.send_message(message)
                _log.info(f"Alarm email sent for {alarm_event.device_id} - {alarm_event.obj_id}")
        except Exception as e:  # Add error handling for email sending
            _log.error(f"Error sending alarm email: {e}")

    def acknowledge_alarm(self, device_id: ObjectIdentifier, obj_id: ObjectIdentifier):
        """Acknowledges an active alarm."""
        key = (device_id, obj_id)
        if key in self.active_alarms:
            self.active_alarms[key].acknowledged = True
            _log.info(f"Alarm acknowledged: {device_id} - {obj_id}")

    def clear_alarm(self, device_id: ObjectIdentifier, obj_id: ObjectIdentifier):
        """Clears an active alarm (when the condition is no longer present)."""
        key = (device_id, obj_id)
        if key in self.active_alarms:
            del self.active_alarms[key]
            _log.info(f"Alarm cleared: {device_id} - {obj_id}")
