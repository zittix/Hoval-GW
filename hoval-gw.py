# This file is just a sample file that does:
# 1) Open CAN bus can0 on the device (for example a raspberry PI using socketcan)
# 2) Listening for simple message coming from Hoval heater TTE-WEZ
# 3) Publishing each recognized data points to a MQTT broker located at IP 192.168.0.96 with username/password hoval/hoval

import can
import asyncio
import logging
import paho.mqtt.client as mqtt
import time
import json

logging.basicConfig(level=logging.DEBUG)

# Change this to match your Home-Assistant / MQTT broker
broker = '192.168.3.7'
broker_username = "hoval"
broker_password = "hoval"

# If you want data to be periodically queried, add them to below list
# IDs comes from below list
# Warning: for now the device address to poll is fixed since I don't know yet
# how the addresses are assigned
polled_data = [
    (0,0,0), # Outside temperature sensor polling
]

# Polling interval for data (in seconds)
POLLING_INTERVAL = 5

# This is extracted from http://www.hoval.com/misc/TTE/TTE-GW-Modbus-datapoints.xlsx
# (X,Y,Z) means X: Function group, Y: Function number, Z: Datapoint ID
with open('datapoints.json', 'r') as f:
    data_idx = json.load(f)

# Remap data_idx so that the keys are tuples
data_idx = {tuple(e['key']): e['value'] for e in data_idx}

def convert_data(arr, msg):
    if msg[1] == 'U8' or msg[1] == 'U16' or msg[1] == 'U32':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        return val / 10**(msg[2])
    elif msg[1] == 'S8' or msg[1] == 'S16' or msg[1] == 'S32':
        val = int.from_bytes(arr, byteorder='big', signed=True)
        return val / 10**(msg[2])
    elif  msg[1] == 'LIST':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        if len(msg) > 3 and str(val) in msg[3]:
            return msg[3][str(val)]
        return val
    elif msg[1] == 'STR':
        return arr.decode('utf-8')
    else:
        print('Unknown ', msg[1])
        return None

def parse_can_id(id):
    # First 2 bytes, messages prio & offsets
    # Last 2 bytes device type and device ID??
    return (id >> 16, (id >> 8) & 0xff, id & 0xff)

pending_msg = {}

devices = {}

REQUEST = 0x40
ANSWER = 0x42
SET_REQUEST = 0x46

def interpret_message(data):
    if data[0] == ANSWER: # Answer from request
        # Data point
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        if idp in data_idx:
            point = data_idx[idp]
            out = convert_data(data[5:], point)
            if out:
                return (point[0], out)
        else:
            logging.error("No known point found for (%d,%d,%d), len %d", function_group, function_number, datapoint, len(data))
    elif data[0] == SET_REQUEST:
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        if idp in data_idx:
            logging.debug("Setting data %s", data_idx[idp][0])
        else:
            logging.debug("Setting data %s", idp)
    elif data[0] == REQUEST:
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        if idp in data_idx:
            logging.debug("Requesting %s", data_idx[idp][0])
        else:
            logging.debug("Requesting %s", idp)
    else:
        logging.debug("Unknown op code: 0x%02x", data[0])

def query(id):
    """Send a query for the provided id a 3-tuple"""
    assert len(id) == 3
    data = (
        int.to_bytes(0x01, 1, byteorder='big') +
        int.to_bytes(REQUEST, 1, byteorder='big') +
        int.to_bytes(id[0], 1, byteorder='big') +
        int.to_bytes(id[1], 1, byteorder='big') +
        int.to_bytes(id[2], 2, byteorder='big')
    )
    return data

def parse(msg):
    id = parse_can_id(msg.arbitration_id)
    # logging.info("%x (%x,%x)", id[0], id[1], id[2])
    msg_id = id[0] >> 8
    if id[1] != 0x0f or id[2] != 0xff:
        # Message to a device with device ID / device type? Not sure
        if (id[1],id[2]) not in devices:
            logging.info("Message to device type / id: %02x,%02x", id[1], id[2])
            devices[(id[1],id[2])] = True
        return None

    if msg_id == 0x1f:
        if len(msg.data) >= 2:
            # Start of a message

            # Number of CAN message we need to get to rebuild this message, 0 is none.
            msg_len = msg.data[0] >> 3
            if msg_len == 0:
                try:
                    return interpret_message(msg.data[1:])
                except:
                    logging.exception(msg.data)
                    return None
            else:
                msg_header = msg.data[1]
                pending_msg[msg_header] = {
                    "data": msg.data[2:],
                    "nb_remaining": msg_len - 1
                }
        else:
            logging.error("Message too small")
    else:
        # Message part
        msg_header = msg.data[0]
        # Check if we are expecting it
        if msg_header in pending_msg:
            pending_msg[msg_header]["data"] = pending_msg[msg_header]["data"] + msg.data[1:]
            pending_msg[msg_header]["nb_remaining"] -= 1
            if pending_msg[msg_header]["nb_remaining"] == 0:
                # Done receiving the big message, yeah
                # Remove the CRC bytes (Not sure what type of CRC it is..)
                data = pending_msg[msg_header]["data"][:-2]
                del pending_msg[msg_header]
                try:
                    return interpret_message(data)
                except:
                    logging.exception(data)
                    return None
    return None

async def main():
    can0 = can.Bus(channel='can0', bustype='socketcan', receive_own_messages=False)
    reader = can.AsyncBufferedReader()
    logger = can.Logger('canlog.log')

    listeners = [
        reader,         # AsyncBufferedReader() listener
        logger          # Regular Listener object
    ]
    # Create Notifier with an explicit loop to use for scheduling of callbacks
    loop = asyncio.get_event_loop()
    notifier = can.Notifier(can0, listeners, loop=loop)

    client = mqtt.Client("hoval-client")
    client.username_pw_set(username=broker_username,password=broker_password)
    try:
        client.connect(broker)
    except:
        time.sleep(30)
        client.connect(broker)

    last_query = time.time()

    while True:
        polled_data
        # Wait for next message from AsyncBufferedReader
        msg = await reader.get_message()
        parsed = parse(msg)
        if parsed:
            logging.info(parsed)
            ret=client.publish("hoval-gw/"+parsed[0], parsed[1])
            if ret[0] != 0:
                connected = False
                while not connected:
                    try:
                        client.connect(broker)
                        connected = True
                    except:
                        time.sleep(5)


        if time.time() - last_query >= POLLING_INTERVAL:
            start_id = 0
            for i in polled_data:
                data = query(i)
                try:
                    arb_id = start_id % 0x10
                    arb_id = (0x1F0 + arb_id) << 16
                    arb_id += 0x0801 # This is the fixed address?
                    msg = can.Message(arbitration_id=arb_id,
                        data=list(data),
                        is_extended_id=True)
                    can0.send(msg)
                    start_id += 1
                except can.CanError as e:
                    logging.exception(e)
            last_query = time.time()

    # Clean-up
    notifier.stop()
    can0.shutdown()

# Get the default event loop
loop = asyncio.get_event_loop()
# Run until main coroutine finishes
loop.run_until_complete(main())
loop.close()
