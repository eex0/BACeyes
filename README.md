# BACee
Modular, Python BACnet BBMD's communication and COV subscription management System.

uses: bacpypes - https://github.com/JoelBender/bacpypes

Work in progress!

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
