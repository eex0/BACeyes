# BACee
Modular BACnet BBMD's communication and COV subscription reporting.

uses: bacpypes - https://github.com/JoelBender/bacpypes

Work in progress!

Here's a breakdown of the key functionalities:

    Discovering BACnet Devices: The code can find and list BACnet devices on the network, 
    providing information about each device (ID, name, address, etc.). This is essential 
    for knowing what devices are available to interact with.

    Change of Value (COV) Subscription Management: It allows you to subscribe to 
    notifications when specific properties (like temperature, status, etc.) on a BACnet 
    device change.  
    
    This is a powerful mechanism for monitoring real-time data without constantly polling 
    the devices.

Classes and Their Roles

    CustomCOVApplication:
        This is the heart of the BACnet application.
        It handles incoming and outgoing BACnet messages, specifically focusing on:
            Who-Is/I-Am requests for device discovery
            SubscribeCOVRequests and UnsubscribeCOVRequests to manage subscriptions
            Sending ConfirmedCOVNotificationRequests when a subscribed property's value changes.
        It keeps track of subscribed properties and their current values to detect changes.
        It automatically renews subscriptions that have a limited lifetime.

    BACnetClient:
        This class provides a user-friendly interface to interact with the BACnet network.
        Its main methods are:
            discover_devices(): Initiates the device discovery process and returns information about found devices.
            read_property(): Reads the value of a specific property on a device.
            write_property(): Writes a new value to a property (if allowed).
            subscribe_to_changes(): Subscribes to notifications for a property's value changes.

Main Function (How It Works)

    Initialization:
        It sets up a BACnetClient instance connected to the specified BBMD address.
        A device name and ID are assigned to the client.
    Discovery:
        app.discover_remote_devices() is called to initiate the Who-Is/I-Am discovery process, finding all BACnet devices (including BBMDs) on the network.
        client.discover_devices() is called to gather and store details of the discovered devices.
    Testing and Demo:
        The code checks if any devices were found and logs a success or failure message accordingly.
        If devices are found, it subscribes to the presentValue property (if it exists) of each device. This subscription will be active for 60 seconds due to the lifetime parameter.
        A cov_callback function is defined to handle the incoming COV notifications. It simply logs the details of the changed property.

Additional Notes:

    Subscription Management: The code implements an efficient way to manage multiple subscriptions by associating them with their object identifiers and subscriber process identifiers.
    Timer-Based Polling: To detect changes in property values, the check_object_values method is called periodically using a timer. It compares the current property value with its previous value and sends notifications only when there's a change.
    Unsubscribe Handling: The do_UnsubscribeCOVRequest method allows clients to gracefully unsubscribe from COV notifications, and it also stops the timer if there are no active subscriptions for a specific object.

Overall, this code serves as a robust foundation for building BACnet applications that require real-time monitoring and control of devices across potentially complex network topologies.
It can be customized and expanded to suit the specific requirements of various BACnet-based systems.
