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

logging.basicConfig(level=logging.INFO)

# Change this to match your Home-Assistant / MQTT broker
broker = '192.168.3.7'
broker_username = "hoval"
broker_password = "hoval"

# Plant route observed in every local capture; protocol framing is added below.
CAN_REQUEST_ROUTE = 0x00200801

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
    (1,0,7044),
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
    # Kept for backwards-compat; unit now comes from the datapoint definition
    # itself (datapoints.json). This helper is no longer called.
    return None

# Map raw unit strings (as they appear in datapoints.json / db.json) to
# Home Assistant device_class values when applicable.
_UNIT_TO_DEVICE_CLASS = {
    '°C': 'temperature',
    '°F': 'temperature',
    'K':   'temperature',
    'bar': 'pressure',
    'mbar': 'pressure',
    'Pa':  'pressure',
    'hPa': 'pressure',
    'kPa': 'pressure',
    '%':   None,        # could be humidity/battery/etc — leave unset
    'W':   'power',
    'kW':  'power',
    'Wh':  'energy',
    'kWh': 'energy',
    'MWh': 'energy',
    'V':   'voltage',
    'mV':  'voltage',
    'A':   'current',
    'mA':  'current',
    'Hz':  'frequency',
    'rpm': None,
    'h':   'duration',
    'min': 'duration',
    's':   'duration',
    'l/min': 'volume_flow_rate',
    'L/min': 'volume_flow_rate',
    'm³/h': 'volume_flow_rate',
    'l':   'volume',
    'L':   'volume',
    'm³':  'volume',
}

def build_discovery_payload(key, point):
    """Build a Home Assistant MQTT discovery payload for a datapoint."""
    name = point[0]
    point_type = point[1]
    unit = point[3] if len(point) > 3 else None
    options = point[4] if len(point) > 4 else None
    slug = slugify(name)
    state_topic = f"{MQTT_STATE_PREFIX}/{name}"
    payload = {
        "name": name,
        "state_topic": state_topic,
        "unique_id": f"{HA_NODE_ID}_{key[0]}_{key[1]}_{key[2]}_{slug}",
        "object_id": f"{HA_NODE_ID}_{slug}",
        "device": HA_DEVICE,
    }
    if unit and point_type != 'LIST':
        payload["unit_of_measurement"] = unit
        payload["state_class"] = "measurement"
        # cumulative counters (energy/volume) are better tagged total_increasing
        if unit in ('Wh', 'kWh', 'MWh', 'l', 'L', 'm³'):
            payload["state_class"] = "total_increasing"
        dc = _UNIT_TO_DEVICE_CLASS.get(unit)
        if dc:
            payload["device_class"] = dc
    if point_type == 'LIST' and isinstance(options, dict):
        # Values published are the mapped string labels -> expose as enum
        payload["device_class"] = "enum"
        payload["options"] = list(options.values())
        payload.pop("state_class", None)
    return payload

def schedule_state_topic(object_id):
    return f"{MQTT_STATE_PREFIX}/schedule/{object_id:04X}"

def build_schedule_discovery_payload(key):
    object_id = key[2]
    kind, name = _schedule_object_info(object_id)
    state_topic = schedule_state_topic(object_id)
    return {
        "name": name,
        "state_topic": state_topic,
        "value_template": "{{ value_json.name or value_json.object_name }}",
        "json_attributes_topic": state_topic,
        "unique_id": f"{HA_NODE_ID}_schedule_{object_id:04x}",
        "object_id": f"{HA_NODE_ID}_{slugify(name)}",
        "icon": "mdi:calendar-week" if kind.endswith('_week') else "mdi:calendar-clock",
        "device": HA_DEVICE,
    }

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
    try:
        if key[0] == OBJECT_SERVICE:
            payload = build_schedule_discovery_payload(key)
            unique_suffix = f"schedule_{key[2]:04x}"
            name = payload["name"]
        elif point:
            slug = slugify(point[0])
            unique_suffix = f"{key[0]}_{key[1]}_{key[2]}_{slug}"
            payload = build_discovery_payload(key, point)
            name = point[0]
        else:
            return
        topic = f"{HA_DISCOVERY_PREFIX}/sensor/{HA_NODE_ID}/{unique_suffix}/config"
        await mqtt_client.publish(topic, json.dumps(payload), retain=True)
        _ha_announced.add(key)
        logging.info("Announced HA entity for %s", name)
    except Exception:
        logging.exception("Failed to publish discovery for %s", key)

def convert_data(arr, msg):
    if msg[1] == 'U8' or msg[1] == 'U16' or msg[1] == 'U32':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        return val / 10**(msg[2])
    elif msg[1] == 'S8' or msg[1] == 'S16' or msg[1] == 'S32':
        val = int.from_bytes(arr, byteorder='big', signed=True)
        return val / 10**(msg[2])
    elif  msg[1] == 'LIST':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        if len(msg) > 4 and isinstance(msg[4], dict) and str(val) in msg[4]:
            return msg[4][str(val)]
        return val
    elif msg[1] == 'STR':
        return arr.rstrip(b'\0').decode('latin-1')
    elif msg[1] == 'RAW':
        return arr.hex().upper()
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
        if len(msg) > 4 and isinstance(msg[4], dict) and value in list(msg[4].values()):
            id = list(msg[4].keys())[list(msg[4].values()).index(value)]
            return int.to_bytes(id, byteorder='big', signed=False)
        return int.to_bytes(value, byteorder='big', signed=False)
    elif msg[1] == 'STR':
        return value.encode('latin-1')
    else:
        print('Unknown ', msg[1])
        return None

pending_msg = {}

devices = {}

REQUEST = 0x40
ANSWER = 0x42
SET_ANSWER = 0x44
SET_REQUEST = 0x46
OBJECT_SERVICE = 0x62

CAN_ROUTE_MASK = 0x003FFFFF
CAN_LAST_FRAME = 0x00800000
CAN_MULTI_FRAME = 0x1F400000
CAN_SINGLE_FRAME = 0x1FC00000

_WEEKDAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
# Schedule object IDs observed in candump_with_schedule.log's 0x62 object-service traffic.
SCHEDULE_OBJECTS = (
    0x3D10, 0x3D11,  # Heating 1 week programmes 1-2
    0x3D30, 0x3D31,  # Hot-water week programmes 1-2
    0x3E10, 0x3E11, 0x3E12, 0x3E13, 0x3E14,  # Heating 1 day programmes 1-5
    0x3E30, 0x3E31, 0x3E32, 0x3E33, 0x3E34,  # Hot-water day programmes 1-5
)

def _schedule_field_name(kind, tag):
    if tag == 0xFA0F:
        return 'name'
    if kind.endswith('_week') and 0xFA00 <= tag <= 0xFA06:
        return f'{_WEEKDAYS[tag - 0xFA00]} programme id'
    if kind.endswith('_week') and tag == 0xFA07:
        return 'reserved'
    if kind.endswith('_day') and 0xFA14 <= tag <= 0xFA1F:
        return f'point {(tag - 0xFA14) // 2 + 1} {"time" if tag % 2 == 0 else "temperature"}'
    if kind == 'heating_day' and 0xFA28 <= tag <= 0xFA2D:
        return f'point {tag - 0xFA28 + 1} cooling temperature'
    return f'unknown field 0x{tag:04X}'

def _schedule_object_info(object_id):
    ranges = (
        (0x3D10, 0x3D11, 'heating_week', 'Heating 1 - Week programme'),
        (0x3D30, 0x3D31, 'hot_water_week', 'Hot water - Week programme'),
        (0x3E10, 0x3E14, 'heating_day', 'Heating 1 - Day programme'),
        (0x3E30, 0x3E34, 'hot_water_day', 'Hot water - Day programme'),
    )
    for first, last, kind, label in ranges:
        if first <= object_id <= last:
            return kind, f'{label} {object_id - first + 1}'
    raise ValueError(f'Unknown schedule object 0x{object_id:04X}')

def _schedule_field_value(kind, tag, raw):
    if tag == 0xFA0F:
        return raw.rstrip(b'\0').decode('latin-1')
    if len(raw) != 1:
        return raw.hex().upper()
    value = raw[0]
    if kind.endswith('_day') and 0xFA14 <= tag <= 0xFA1F:
        if tag % 2 == 0:
            minutes = value * 15
            return f'{minutes // 60:02d}:{minutes % 60:02d}'
        return value / 2
    if kind == 'heating_day' and 0xFA28 <= tag <= 0xFA2D:
        return value / 2
    return value

def _parse_schedule_fields(object_id, data, field_count):
    kind, _ = _schedule_object_info(object_id)
    fields = {}
    offset = 0
    for _ in range(field_count):
        if offset + 3 > len(data):
            raise ValueError('Truncated schedule field')
        tag = int.from_bytes(data[offset:offset + 2], 'big')
        offset += 2
        length = data[offset] if tag == 0xFA0F else 1
        if tag == 0xFA0F:
            offset += 1
        if offset + length > len(data):
            raise ValueError(f'Truncated schedule field 0x{tag:04X}')
        raw = data[offset:offset + length]
        fields[tag] = _schedule_field_value(kind, tag, raw)
        offset += length
    if offset != len(data):
        raise ValueError(f'Unexpected trailing schedule data ({len(data) - offset} bytes)')
    return fields

def _schedule_state(object_id, fields):
    kind, object_name = _schedule_object_info(object_id)
    state = {
        'object_id': f'{object_id:04X}',
        'object_name': object_name,
        'kind': kind,
        'name': fields.get(0xFA0F),
        'fields': {f'{tag:04X}': value for tag, value in fields.items()},
        'field_names': {f'{tag:04X}': _schedule_field_name(kind, tag) for tag in fields},
    }
    if kind.endswith('_week'):
        state['days'] = {
            day: fields[0xFA00 + index]
            for index, day in enumerate(_WEEKDAYS)
            if 0xFA00 + index in fields
        }
    else:
        points = []
        for index in range(6):
            time_tag = 0xFA14 + index * 2
            temperature_tag = time_tag + 1
            if time_tag not in fields:
                continue
            point = {'time': fields[time_tag], 'temperature': fields.get(temperature_tag)}
            cooling_tag = 0xFA28 + index
            if cooling_tag in fields:
                point['cooling_temperature'] = fields[cooling_tag]
            points.append(point)
        state['points'] = points
    return state

def decode_schedule_datapoint(data):
    """Decode the normal read/set envelope used for individual schedule fields."""
    data = bytes(data)
    if len(data) < 5 or data[0] not in (REQUEST, ANSWER, SET_ANSWER, SET_REQUEST):
        return None
    object_id = int.from_bytes(data[1:3], 'big')
    try:
        kind, object_name = _schedule_object_info(object_id)
    except ValueError:
        return None
    operation = {
        REQUEST: 'field_read',
        ANSWER: 'field_read_response',
        SET_REQUEST: 'field_set',
        SET_ANSWER: 'field_set_ack',
    }[data[0]]
    if operation in ('field_read', 'field_set_ack') and len(data) != 5:
        raise ValueError('Invalid schedule datapoint request or acknowledgement')
    if operation in ('field_read_response', 'field_set') and len(data) < 6:
        raise ValueError('Invalid schedule datapoint value')
    tag = int.from_bytes(data[3:5], 'big')
    result = {
        'operation': operation,
        'object_id': f'{object_id:04X}',
        'object_name': object_name,
        'field': f'{tag:04X}',
        'field_name': _schedule_field_name(kind, tag),
    }
    if len(data) > 5:
        result['value'] = _schedule_field_value(kind, tag, data[5:])
    return result

def decode_object_service(data):
    """Decode every 0x62 object-service message observed in the captures."""
    data = bytes(data)
    if len(data) < 2 or data[0] != OBJECT_SERVICE:
        raise ValueError('Not an object-service message')
    operation = data[1]

    if operation == 0x01:
        if len(data) != 4:
            raise ValueError('Invalid datapoint catalogue request')
        return {'operation': 'catalogue_read', 'function_group': data[2], 'function_number': data[3]}

    if operation == 0x81:
        if len(data) < 7 or len(data) != 7 + data[6] * 3:
            raise ValueError('Invalid datapoint catalogue response')
        function_group, function_number = data[2:4]
        entries = []
        for offset in range(7, len(data), 3):
            datapoint = int.from_bytes(data[offset:offset + 2], 'big')
            entry = {'id': datapoint, 'descriptor': f'{data[offset + 2]:02X}'}
            definition = data_idx.get((function_group, function_number, datapoint))
            if definition:
                entry.update(name=definition[0], type=definition[1])
            else:
                entry.update(
                    name=f'Unknown datapoint {function_group}/{function_number}/{datapoint}',
                    type='RAW',
                )
            entries.append(entry)
        return {
            'operation': 'catalogue_response',
            'function_group': function_group,
            'function_number': function_number,
            'offset': int.from_bytes(data[4:6], 'big'),
            'entries': entries,
        }

    if len(data) < 4:
        raise ValueError('Truncated object-service message')
    object_id = int.from_bytes(data[2:4], 'big')

    # 0x06/0x86 request/response pairs observed live on the gateway. Their
    # values are not documented, so retain the selector and value as raw hex.
    if operation in (0x06, 0x86):
        if len(data) < 6 or (operation == 0x06 and len(data) != 6):
            raise ValueError('Invalid object-value request or response')
        result = {
            'operation': 'object_value_request' if operation == 0x06 else 'object_value_response',
            'object_id': f'{object_id:04X}',
            'selector': data[4:6].hex().upper(),
        }
        if operation == 0x86:
            result['value'] = data[6:].hex().upper()
        return result

    try:
        kind, object_name = _schedule_object_info(object_id)
    except ValueError:
        return {
            'operation': 'unknown_object_service',
            'opcode': f'{operation:02X}',
            'object_id': f'{object_id:04X}',
            'payload': data[4:].hex().upper(),
        }
    result = {'object_id': f'{object_id:04X}', 'object_name': object_name, 'kind': kind}

    if operation == 0x00:
        if len(data) != 8:
            raise ValueError('Invalid schedule read request')
        result.update(operation='read', selector=data[4:].hex().upper())
    elif operation == 0x80:
        if len(data) < 7:
            raise ValueError('Invalid schedule response')
        fields = _parse_schedule_fields(object_id, data[7:], data[6])
        result.update(operation='read_response', selector=data[4:6].hex().upper(), schedule=_schedule_state(object_id, fields))
    elif operation == 0x03:
        if len(data) != 4:
            raise ValueError('Invalid schedule transaction start')
        result['operation'] = 'transaction_start'
    elif operation == 0x04:
        if len(data) != 5:
            raise ValueError('Invalid schedule commit')
        result.update(operation='commit', value=data[4])
    elif operation == 0x05:
        if len(data) < 7 or len(data) != 7 + data[6]:
            raise ValueError('Invalid schedule field write')
        tag = int.from_bytes(data[4:6], 'big')
        result.update(
            operation='field_write',
            field=f'{tag:04X}',
            field_name=_schedule_field_name(kind, tag),
            value=_schedule_field_value(kind, tag, data[7:]),
        )
    elif operation == 0x09:
        if len(data) < 5:
            raise ValueError('Invalid schedule replacement')
        fields = _parse_schedule_fields(object_id, data[5:], data[4])
        result.update(operation='replace', schedule=_schedule_state(object_id, fields))
    elif operation in (0x83, 0x84):
        if len(data) != 5:
            raise ValueError('Invalid schedule acknowledgement')
        result.update(operation='transaction_ack' if operation == 0x83 else 'commit_ack', status=data[4])
    elif operation == 0x85:
        if len(data) != 7:
            raise ValueError('Invalid schedule field acknowledgement')
        tag = int.from_bytes(data[4:6], 'big')
        result.update(operation='field_ack', field=f'{tag:04X}',
                      field_name=_schedule_field_name(kind, tag), status=data[6])
    elif operation == 0x89:
        if len(data) < 5 or len(data) != 5 + data[4]:
            raise ValueError('Invalid schedule replacement acknowledgement')
        result.update(operation='replace_ack', statuses=list(data[5:]))
    else:
        raise ValueError(f'Unknown object-service operation 0x{operation:02X}')
    return result

def interpret_message(data):
    if not data:
        return None
    schedule_datapoint = decode_schedule_datapoint(data)
    if schedule_datapoint:
        logging.debug('Schedule datapoint operation: %s', schedule_datapoint['operation'])
    elif data[0] == OBJECT_SERVICE:
        decoded = decode_object_service(data)
        if decoded['operation'] == 'read_response':
            schedule = decoded['schedule']
            return ((OBJECT_SERVICE, 0, int(schedule['object_id'], 16)),
                    schedule['object_name'], json.dumps(schedule, ensure_ascii=False))
        logging.debug('Object service operation: %s', decoded['operation'])
    elif data[0] == ANSWER: # Answer from request
        # Data point
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        point = data_idx.get(idp)
        if not point:
            name = f'Unknown datapoint {function_group}/{function_number}/{datapoint}'
            logging.warning('%s, preserving %d raw bytes', name, len(data) - 5)
            return (idp, name, data[5:].hex().upper())
        out = convert_data(data[5:], point)
        if out is not None:
            return (idp, point[0], out)
    elif data[0] in (REQUEST, SET_REQUEST, SET_ANSWER):
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        action = {REQUEST: 'Requesting', SET_REQUEST: 'Setting', SET_ANSWER: 'Set acknowledged'}[data[0]]
        logging.debug('%s %s', action, data_idx.get(idp, [idp])[0])
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

def query_schedule(object_id):
    _schedule_object_info(object_id)
    return (
        b'\x01'  # TopTronic message marker consumed by build_can_frames()
        + b'\x62\x00'  # Object service (0x62), read operation (0x00)
        + object_id.to_bytes(2, 'big')  # 16-bit schedule object ID
        + b'\x00\x00\xff\xff'  # Captured whole-object selector; inner semantics unknown
    )

def toptronic_crc(data):
    """Return the TopTronic multi-frame CRC-16."""
    crc = 0x600D  # Reflected form of init=0xB006, poly=0x1021.
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x8408 if crc & 1 else 0)
    return crc

def verify_crc(msg_data):
    return len(msg_data) >= 2 and toptronic_crc(msg_data[:-2]) == int.from_bytes(msg_data[-2:], 'big')

def build_can_frames(arbitration_id, data, message_header):
    """Split a TopTronic request into one or more CAN frames."""
    data = bytes(data)
    if not data or data[0] != 0x01:
        raise ValueError("TopTronic messages must start with 0x01")
    if arbitration_id >> 24 != 0x1f:
        raise ValueError("TopTronic arbitration IDs must start with 0x1F")
    route_id = arbitration_id & CAN_ROUTE_MASK
    if len(data) <= 8:
        return [(CAN_SINGLE_FRAME | route_id, data)]
    if not 0 <= message_header <= 0xff:
        raise ValueError("Multi-frame message header must fit in one byte")

    payload = data[1:]
    payload += toptronic_crc(payload).to_bytes(2, 'big')
    frame_count = 1 + (max(0, len(payload) - 6) + 6) // 7
    if frame_count > 31:
        raise ValueError("TopTronic message is too large")

    frames = [(CAN_MULTI_FRAME | route_id,
               bytes([(frame_count << 3) | 0x01, message_header]) + payload[:6])]
    for frame_index, offset in enumerate(range(6, len(payload), 7), 1):
        frame_id = ((0x1f - frame_index) << 24) | route_id
        if frame_index == frame_count - 1:
            frame_id |= CAN_LAST_FRAME
        frames.append((frame_id, bytes([message_header]) + payload[offset:offset + 7]))
    return frames

_message_header = 0

def send_can_data(can_bus, arbitration_id, data):
    global _message_header
    _message_header = (_message_header + 1) & 0xff
    for frame_id, frame_data in build_can_frames(arbitration_id, data, _message_header):
        can_bus.send(can.Message(arbitration_id=frame_id,
                                 data=frame_data,
                                 is_extended_id=True))

def parse(msg):
    msg_id = msg.arbitration_id >> 24
    route_id = msg.arbitration_id & CAN_ROUTE_MASK
    device_type = (msg.arbitration_id >> 8) & 0xff
    device_id = msg.arbitration_id & 0xff
    if device_type != 0x0f or device_id != 0xff:
        # Message to a device with device ID / device type? Not sure
        if (device_type, device_id) not in devices:
            logging.info("Message to device type / id: %02x,%02x", device_type, device_id)
            devices[(device_type, device_id)] = True

    if msg_id == 0x1f:
        if len(msg.data) >= 2:
            # Start of a message

            # Number of CAN message we need to get to rebuild this message, 0 is none.
            msg_len = msg.data[0] >> 3
            if msg_len == 0:
                try:
                    return interpret_message(msg.data[1:])
                except Exception:
                    logging.exception(msg.data)
                    return None
            else:
                msg_header = msg.data[1]
                pending_msg[(route_id, msg_header)] = {
                    "data": msg.data[2:],
                    "nb_remaining": msg_len - 1
                }
        else:
            logging.error("Message too small")
    elif len(msg.data) >= 2:
        # Message part
        msg_header = msg.data[0]
        pending_key = (route_id, msg_header)
        # Check if we are expecting it
        if pending_key in pending_msg:
            pending_msg[pending_key]["data"] = pending_msg[pending_key]["data"] + msg.data[1:]
            pending_msg[pending_key]["nb_remaining"] -= 1
            if pending_msg[pending_key]["nb_remaining"] == 0:
                # Done receiving the big message, yeah
                data = pending_msg[pending_key]["data"]
                del pending_msg[pending_key]
                if not verify_crc(data):
                    logging.error("Invalid CRC for multi-frame message")
                    return None
                data = data[:-2]
                try:
                    return interpret_message(data)
                except Exception:
                    logging.exception(data)
                    return None
    return None

async def read_can_bus(can_bus, mqtt_client):
    reader = can.AsyncBufferedReader()
    #listener = can.Logger()
    notifier = can.Notifier(can_bus, [reader])
    last_query = time.time()
    schedule_index = 0
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
                state_topic = (schedule_state_topic(key[2]) if key[0] == OBJECT_SERVICE
                               else f"{MQTT_STATE_PREFIX}/{name}")
                # Discovery and state are published back-to-back; retain schedule
                # state so Home Assistant cannot miss it while creating the entity.
                await mqtt_client.publish(state_topic, value, retain=key[0] == OBJECT_SERVICE)

            if time.time() - last_query >= POLLING_INTERVAL:
                for i in polled_data:
                    data = query(i)
                    try:
                        send_can_data(can_bus, CAN_SINGLE_FRAME | CAN_REQUEST_ROUTE, data)
                    except can.CanError as e:
                        logging.exception(e)
                try:
                    send_can_data(can_bus, CAN_SINGLE_FRAME | CAN_REQUEST_ROUTE,
                                  query_schedule(SCHEDULE_OBJECTS[schedule_index]))
                    schedule_index = (schedule_index + 1) % len(SCHEDULE_OBJECTS)
                except can.CanError as e:
                    logging.exception(e)
                last_query = time.time()
    finally:
        notifier.stop()

async def handle_mqtt_messages(can_bus, mqtt_client):
    async with mqtt_client.messages() as messages:
        await mqtt_client.subscribe(MQTT_TOPIC_SUBSCRIBE)
        async for msg in messages:
            if msg.topic.matches(MQTT_TOPIC_SUBSCRIBE):
                print('Received MQTT message:', msg.payload)
                can_data = None
                try:
                    can_data = json.loads(msg.payload)
                except (TypeError, ValueError):
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
                        send_can_data(can_bus, CAN_SINGLE_FRAME | CAN_REQUEST_ROUTE, data)
                    except can.CanError as e:
                        logging.exception(e)

async def main():
    while True:
        can_bus = None
        try:
            mqtt_client = MQTTClient(broker, username=broker_username,password=broker_password)
            can_bus = can.Bus(channel='can0', bustype='socketcan', receive_own_messages=False)
            async with mqtt_client:
                await asyncio.gather(
                    read_can_bus(can_bus, mqtt_client),
                    handle_mqtt_messages(can_bus, mqtt_client)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception('Error in main loop')
        finally:
            if can_bus is not None:
                can_bus.shutdown()
        await asyncio.sleep(2)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
