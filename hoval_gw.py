# -*- coding: utf-8 -*-
# This file is just a sample file that does:
# 1) Open CAN bus can0 on the device (for example a raspberry PI using socketcan)
# 2) Listening for simple message coming from Hoval heater TTE-WEZ
# 3) Publishing each recognized data points to a MQTT broker located at IP 192.168.0.96 with username/password hoval/hoval

import can
import asyncio
import logging
from asyncio_mqtt import Client as MQTTClient
import asyncio_mqtt
import time
import json
import re
import unicodedata

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
    (1,0,2),
    (1,0,1),
    (1,0,1002),
    (1,0,2051),
]

writable_data = [
    (1,0,1),
]

MQTT_TOPIC_SUBSCRIBE = 'hoval_write'

# Polling interval for data (in seconds)
POLLING_INTERVAL = 30

# This is extracted from http://www.hoval.com/misc/TTE/TTE-GW-Modbus-datapoints.xlsx
# (X,Y,Z) means X: Function group, Y: Function number, Z: Datapoint ID
with open('datapoints.json', 'r') as f:
    data_idx = json.load(f)

# Remap data_idx so that the keys are tuples
data_idx = {tuple(e['key']): e['value'] for e in data_idx}
writable_data = {data_idx[e][0]: e for e in writable_data}

# ---------------------------------------------------------------------------
# Home Assistant MQTT discovery
# ---------------------------------------------------------------------------
HA_DISCOVERY_PREFIX = 'homeassistant'
HA_NODE_ID = 'hoval_gw'
MQTT_STATE_PREFIX = 'hoval-gw'

# Device block sent with every discovery message so HA groups all entities
# under a single "Hoval Gateway" device.
HA_DEVICE = {
    "identifiers": [HA_NODE_ID],
    "name": "Hoval Gateway",
    "manufacturer": "Hoval",
    "model": "TTE-GW (CAN)",
}

def slugify(name):
    """Return an ASCII slug usable as an MQTT object_id / unique_id."""
    # Strip accents
    normalized = unicodedata.normalize('NFKD', name)
    ascii_name = normalized.encode('ascii', 'ignore').decode('ascii')
    ascii_name = ascii_name.lower()
    # Replace anything not [a-z0-9] with underscore
    slug = re.sub(r'[^a-z0-9]+', '_', ascii_name).strip('_')
    return slug or 'point'

def _guess_unit(name, point_type):
    """Best-effort unit detection from the datapoint name."""
    if point_type in ('STR', 'LIST'):
        return None
    n = name.lower()
    if 'temp' in n or 'sonde' in n or '°c' in n:
        return '°C'
    if 'pression' in n or 'pressure' in n:
        return 'bar'
    if 'humid' in n:
        return '%'
    if 'puissance' in n or 'power' in n:
        return 'W'
    if 'debit' in n or 'flow' in n or 'débit' in n:
        return 'L/min'
    if 'heure' in n or 'hours' in n or 'runtime' in n:
        return 'h'
    if 'vitesse' in n or 'speed' in n or 'rpm' in n:
        return 'rpm'
    return None

def build_discovery_payload(key, point):
    """Build a Home Assistant MQTT discovery payload for a datapoint."""
    name = point[0]
    point_type = point[1]
    slug = slugify(name)
    state_topic = f"{MQTT_STATE_PREFIX}/{name}"
    payload = {
        "name": name,
        "state_topic": state_topic,
        "unique_id": f"{HA_NODE_ID}_{key[0]}_{key[1]}_{key[2]}_{slug}",
        "object_id": f"{HA_NODE_ID}_{slug}",
        "device": HA_DEVICE,
    }
    unit = _guess_unit(name, point_type)
    if unit:
        payload["unit_of_measurement"] = unit
        payload["state_class"] = "measurement"
        if unit == '°C':
            payload["device_class"] = "temperature"
        elif unit == 'bar':
            payload["device_class"] = "pressure"
        elif unit == '%':
            payload["device_class"] = "humidity"
        elif unit == 'W':
            payload["device_class"] = "power"
    if point_type == 'LIST' and len(point) > 3 and isinstance(point[3], dict):
        # Values published are the mapped string labels -> expose as enum
        payload["device_class"] = "enum"
        payload["options"] = list(point[3].values())
        payload.pop("state_class", None)
    return payload

# Keys (function_group, function_number, datapoint) we've already announced
# to Home Assistant during this process. Discovery is published lazily the
# first time we actually see data for a datapoint, so HA never shows entities
# that the heater never reports.
_ha_announced = set()

async def announce_ha_entity(mqtt_client, key):
    """Publish the HA discovery config for ``key`` once."""
    if key in _ha_announced:
        return
    point = data_idx.get(key)
    if not point:
        return
    try:
        slug = slugify(point[0])
        unique_suffix = f"{key[0]}_{key[1]}_{key[2]}_{slug}"
        topic = f"{HA_DISCOVERY_PREFIX}/sensor/{HA_NODE_ID}/{unique_suffix}/config"
        payload = build_discovery_payload(key, point)
        await mqtt_client.publish(topic, json.dumps(payload), retain=True)
        _ha_announced.add(key)
        logging.info("Announced HA entity for %s", point[0])
    except Exception:
        logging.exception("Failed to publish discovery for %s", point)

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

def convert_value(value, msg):
    type_to_len = {
        'U8': 1,
        'U16': 2,
        'U32': 4,
        'S8': 1,
        'S16': 2,
        'S32': 4,
    }
    if msg[1] == 'U8' or msg[1] == 'U16' or msg[1] == 'U32':
        val = int.to_bytes(int(value * 10**(msg[2])), type_to_len[msg[1]], byteorder='big', signed=False)
        return val 
    elif msg[1] == 'S8' or msg[1] == 'S16' or msg[1] == 'S32':
        val = int.to_bytes(int(value * 10**(msg[2])), type_to_len[msg[1]], byteorder='big', signed=True)
        return val
    elif  msg[1] == 'LIST':
        if len(msg) > 3 and value in list(msg[3].values()):
            id = list(msg[3].keys())[list(msg[3].values()).index(value)]
            return int.to_bytes(id, byteorder='big', signed=False)
        return int.to_bytes(value, byteorder='big', signed=False)
    elif msg[1] == 'STR':
        return value.encode('utf-8')
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
                return (idp, point[0], out)
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

def verify_crc(msg_data):
    pass

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
    elif len(msg.data) >= 2:
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

async def read_can_bus(can_bus, mqtt_client):
    reader = can.AsyncBufferedReader()
    #listener = can.Logger()
    notifier = can.Notifier(can_bus, [reader])
    last_query = time.time()
    try:
        while True:
            # Wait for next message from AsyncBufferedReader
            msg = await reader.get_message()
            parsed = parse(msg)
            if parsed:
                logging.info(parsed)
                key, name, value = parsed
                # Announce the entity to Home Assistant the first time we
                # actually see data for it (retained config), then publish
                # the state on the existing topic.
                await announce_ha_entity(mqtt_client, key)
                await mqtt_client.publish(f"{MQTT_STATE_PREFIX}/"+name, value)

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
                        can_bus.send(msg)
                        start_id += 1
                    except can.CanError as e:
                        logging.exception(e)
                last_query = time.time()
    finally:
        notifier.stop()

async def handle_mqtt_messages(can_bus, mqtt_client):
    async with mqtt_client.filtered_messages(MQTT_TOPIC_SUBSCRIBE) as messages:
        await mqtt_client.subscribe(MQTT_TOPIC_SUBSCRIBE)
        start_id = 0
        async for msg in messages:
            if msg.topic == MQTT_TOPIC_SUBSCRIBE:
                print('Received MQTT message:', msg.payload)
                can_data = None
                try:
                    can_data = json.loads(msg.payload)
                except:
                    pass
                if can_data:
                    message_id=can_data['id']
                    message_value=can_data['value']
                    if message_id not in writable_data:
                        print('Data point not writable')
                        continue
                    can_id = writable_data[message_id]
                    converted_value = convert_value(message_value, data_idx[can_id])
                    if converted_value is None:
                        print('Unable to convert value')
                        continue
                    data = (
                        int.to_bytes(0x01, 1, byteorder='big') +
                        int.to_bytes(SET_REQUEST, 1, byteorder='big') +
                        int.to_bytes(can_id[0], 1, byteorder='big') +
                        int.to_bytes(can_id[1], 1, byteorder='big') +
                        int.to_bytes(can_id[2], 2, byteorder='big') +
                        converted_value
                    )
                    try:
                        arb_id = start_id % 0x10
                        arb_id = (0x1F0 + arb_id) << 16
                        arb_id += 0x0801 # This is the fixed address?
                        msg = can.Message(arbitration_id=arb_id,
                            data=list(data),
                            is_extended_id=True)
                        can_bus.send(msg)
                        start_id += 1
                    except can.CanError as e:
                        logging.exception(e)

async def main():
    while True:
        try:
            mqtt_client = MQTTClient(broker, username=broker_username,password=broker_password)
            can_bus = can.Bus(channel='can0', bustype='socketcan', receive_own_messages=False)
            async with mqtt_client:
                await asyncio.gather(
                    read_can_bus(can_bus, mqtt_client),
                    handle_mqtt_messages(can_bus, mqtt_client)
            )
        except:
            logging.exception('Error in main loop')
        finally:
            can_bus.shutdown()
            await asyncio.sleep(2)

if __name__ == '__main__':
    asyncio.run(main())