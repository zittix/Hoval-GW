import os
import asyncio
import json
import logging
import re
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Protocol helpers do not need the gateway's runtime dependencies.
can = types.ModuleType('can')
can.CanError = Exception
can.Bus = object
can.Message = object
sys.modules.setdefault('can', can)
mqtt = types.ModuleType('asyncio_mqtt')
mqtt.Client = object
mqtt.MqttError = Exception
sys.modules.setdefault('asyncio_mqtt', mqtt)

import hoval_gw


class TestTopTronicProtocol(unittest.TestCase):
    def test_live_object_value_packets_are_preserved(self):
        self.assertEqual(
            hoval_gw.decode_object_service(bytes.fromhex('62 06 01 00 7A 71')),
            {'operation': 'object_value_request', 'object_id': '0100', 'selector': '7A71'},
        )
        self.assertEqual(
            hoval_gw.decode_object_service(bytes.fromhex('62 86 01 00 7A 71 00 00 00 00')),
            {'operation': 'object_value_response', 'object_id': '0100',
             'selector': '7A71', 'value': '00000000'},
        )
        self.assertEqual(
            hoval_gw.decode_object_service(bytes.fromhex('62 86 02 00 04 00 26 02')),
            {'operation': 'object_value_response', 'object_id': '0200',
             'selector': '0400', 'value': '2602'},
        )

    def test_main_does_not_reconnect_when_cancelled(self):
        class Bus:
            stopped = False

            def shutdown(self):
                self.stopped = True

        class MQTT:
            async def __aenter__(self):
                raise asyncio.CancelledError()

            async def __aexit__(self, *args):
                pass

        bus = Bus()
        mqtt_client, can_bus = hoval_gw.MQTTClient, hoval_gw.can.Bus
        hoval_gw.MQTTClient = lambda *args, **kwargs: MQTT()
        hoval_gw.can.Bus = lambda *args, **kwargs: bus
        try:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(hoval_gw.main())
        finally:
            hoval_gw.MQTTClient, hoval_gw.can.Bus = mqtt_client, can_bus
        self.assertTrue(bus.stopped)

    def test_schedule_home_assistant_discovery_and_poll(self):
        key = (hoval_gw.OBJECT_SERVICE, 0, 0x3D10)
        payload = hoval_gw.build_schedule_discovery_payload(key)
        self.assertEqual(payload['state_topic'], 'hoval-gw/schedule/3D10')
        self.assertEqual(payload['json_attributes_topic'], payload['state_topic'])
        self.assertEqual(payload['value_template'], '{{ value_json.name or value_json.object_name }}')
        self.assertNotIn('command_topic', payload)

        class MQTT:
            published = []

            async def publish(self, topic, value, retain=False):
                self.published.append((topic, json.loads(value), retain))

        mqtt = MQTT()
        hoval_gw._ha_announced.discard(key)
        asyncio.run(hoval_gw.announce_ha_entity(mqtt, key))
        self.assertEqual(mqtt.published[0][0], 'homeassistant/sensor/hoval_gw/schedule_3d10/config')
        self.assertTrue(mqtt.published[0][2])

        request = hoval_gw.query_schedule(0x3D10)
        self.assertEqual(request, bytes.fromhex('01 62 00 3D 10 00 00 FF FF'))
        self.assertEqual(hoval_gw.build_can_frames(0x1FE00801, request, 0x64), [
            (0x1F600801, bytes.fromhex('11 64 62 00 3D 10 00 00')),
            (0x1EA00801, bytes.fromhex('64 FF FF 70 C1')),
        ])

    def test_captured_crcs(self):
        samples = {
            '70a60001450f000000000000000000000000000000': 0x481e,
            '70a3000141050100000000': 0xa8ef,
            '7404ff00000000ffff': 0x673b,
            '70a1000152080000dc0000000000': 0x7762,
            '62803d10000009fa0001fa0101fa0201fa0301fa0401fa0501fa0601fa07fffa0f0a53656d61696e65203100': 0x7014,
        }
        for payload, expected in samples.items():
            self.assertEqual(hoval_gw.toptronic_crc(bytes.fromhex(payload)), expected)

    def test_captured_multi_frame_schedule_write(self):
        data = bytes.fromhex(
            '01 62 09 3D 10 09 FA 00 00 FA 01 00 FA 02 00 FA 03 00 FA 04 00 '
            'FA 05 00 FA 06 00 FA 07 FF FA 0F 0A 53 65 6D 61 69 6E 65 20 31 00'
        )
        frames = hoval_gw.build_can_frames(0x1fe00801, data, 0x76)
        self.assertEqual(frames, [
            (0x1f600801, bytes.fromhex('39 76 62 09 3D 10 09 FA')),
            (0x1e200801, bytes.fromhex('76 00 00 FA 01 00 FA 02')),
            (0x1d200801, bytes.fromhex('76 00 FA 03 00 FA 04 00')),
            (0x1c200801, bytes.fromhex('76 FA 05 00 FA 06 00 FA')),
            (0x1b200801, bytes.fromhex('76 07 FF FA 0F 0A 53 65')),
            (0x1a200801, bytes.fromhex('76 6D 61 69 6E 65 20 31')),
            (0x19a00801, bytes.fromhex('76 00 A2 CF')),
        ])

    def test_crc_verification_rejects_corruption(self):
        message = bytes.fromhex('42 01 00 01 F9 53 65 6D 61 69 6E 65 20 31 E8 5A')
        self.assertTrue(hoval_gw.verify_crc(message))
        self.assertFalse(hoval_gw.verify_crc(message[:-1] + b'\x00'))

    def test_all_object_service_messages_in_schedule_capture(self):
        line_pattern = re.compile(r'^\([^)]+\)\s+\S+\s+([0-9A-F]+)#([0-9A-F]*)$')
        pending = {}
        messages = []
        parsed_schedules = []
        capture = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'candump_with_schedule.log')

        previous_log_level = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        hoval_gw.pending_msg.clear()
        try:
            with open(capture) as lines:
                for line_number, line in enumerate(lines, 1):
                    match = line_pattern.match(line.strip())
                    self.assertIsNotNone(match, f'Invalid candump line {line_number}')
                    arbitration_id = int(match.group(1), 16)
                    frame = bytes.fromhex(match.group(2))
                    parsed = hoval_gw.parse(types.SimpleNamespace(arbitration_id=arbitration_id, data=frame))
                    if parsed and parsed[0][0] == hoval_gw.OBJECT_SERVICE:
                        parsed_schedules.append(parsed)
                    message_id = arbitration_id >> 24
                    if len(frame) >= 2 and frame[0] == 1 and message_id in (0x01, 0x1F):
                        messages.append(frame[1:])
                    elif message_id == 0x1F and len(frame) >= 2 and frame[0] >> 3:
                        pending[(arbitration_id & hoval_gw.CAN_ROUTE_MASK, frame[1])] = [(frame[0] >> 3) - 1, bytearray(frame[2:])]
                    elif frame and (arbitration_id & hoval_gw.CAN_ROUTE_MASK, frame[0]) in pending:
                        pending_key = (arbitration_id & hoval_gw.CAN_ROUTE_MASK, frame[0])
                        item = pending[pending_key]
                        item[0] -= 1
                        item[1].extend(frame[1:])
                        if item[0] == 0:
                            payload = bytes(item[1])
                            self.assertTrue(hoval_gw.verify_crc(payload), f'Invalid captured CRC at line {line_number}')
                            messages.append(payload[:-2])
                            del pending[pending_key]
        finally:
            logging.disable(previous_log_level)

        self.assertEqual(len(parsed_schedules), 119)
        self.assertFalse(hoval_gw.pending_msg)

        object_messages = [message for message in messages if message[:1] == b'\x62']
        decoded = [hoval_gw.decode_object_service(message) for message in object_messages]
        self.assertEqual(len(decoded), 350)
        self.assertEqual(
            {item['operation'] for item in decoded},
            {'read', 'catalogue_read', 'transaction_start', 'commit', 'field_write', 'replace',
             'read_response', 'catalogue_response', 'transaction_ack', 'commit_ack', 'field_ack', 'replace_ack'},
        )

        summer = [item['schedule'] for item in decoded
                  if item['operation'] == 'read_response' and item['object_id'] == '3E10']
        self.assertEqual(summer[0]['name'], 'été')
        self.assertEqual(summer[0]['fields']['FA18'], '11:30')
        self.assertEqual(summer[-1]['fields']['FA18'], '11:45')
        self.assertEqual(summer[0]['fields']['FA29'], 20.0)
        self.assertEqual(summer[-1]['fields']['FA29'], 21.0)
        schedule_fields = {
            tag: name
            for item in decoded if 'schedule' in item
            for tag, name in item['schedule']['field_names'].items()
        }
        self.assertEqual(len(schedule_fields), 27)
        self.assertFalse([name for name in schedule_fields.values()
                          if name.startswith('unknown field ')])

        catalogue = next(item for item in decoded
                         if item['operation'] == 'catalogue_response'
                         and item['function_group'] == 1
                         and item['function_number'] == 0
                         and item['offset'] == 0)
        week_programme_id = next(entry for entry in catalogue['entries'] if entry['id'] == 504)
        self.assertEqual(week_programme_id['name'], 'ID current week programme')
        self.assertEqual(week_programme_id['type'], 'U8')

        catalogue_entries = {
            (item['function_group'], item['function_number'], entry['id']): entry
            for item in decoded if item['operation'] == 'catalogue_response'
            for entry in item['entries']
        }
        self.assertEqual(len(catalogue_entries), 679)
        self.assertTrue(all({'id', 'descriptor', 'name', 'type'} <= entry.keys()
                            for entry in catalogue_entries.values()))
        self.assertEqual(sum(entry['name'].startswith('Unknown datapoint ')
                             for entry in catalogue_entries.values()), 587)

        zero = hoval_gw.interpret_message(bytes.fromhex('42 01 00 01 F8 00'))
        self.assertEqual(zero, ((1, 0, 504), 'ID current week programme', 0.0))

        schedule_datapoints = []
        for message in messages:
            decoded = hoval_gw.decode_schedule_datapoint(message)
            if decoded:
                schedule_datapoints.append(decoded)
        self.assertEqual(len(schedule_datapoints), 12)
        self.assertEqual(
            {item['operation'] for item in schedule_datapoints},
            {'field_read', 'field_read_response', 'field_set', 'field_set_ack'},
        )
        self.assertTrue(all(item['field'] == 'FA07' for item in schedule_datapoints))
        self.assertTrue(all(item['field_name'] == 'reserved' for item in schedule_datapoints))

        datapoint_messages = [message for message in messages
                              if message[0] in (hoval_gw.REQUEST, hoval_gw.ANSWER,
                                                hoval_gw.SET_ANSWER, hoval_gw.SET_REQUEST)
                              and not hoval_gw.decode_schedule_datapoint(message)]
        observed_keys = {
            (message[1], message[2], int.from_bytes(message[3:5], 'big'))
            for message in datapoint_messages
        }
        self.assertFalse(observed_keys - hoval_gw.data_idx.keys())

        answers = [message for message in datapoint_messages if message[0] == hoval_gw.ANSWER]
        self.assertEqual(len(answers), 217)
        self.assertTrue(all(hoval_gw.interpret_message(message) is not None for message in answers))
        self.assertEqual(
            {key for key in observed_keys if hoval_gw.data_idx[key][1] == 'RAW'},
            {(1, 2, 7076), (2, 0, 2011), (2, 0, 2018), (2, 0, 3078), (10, 1, 1100)},
        )

        self.assertEqual(
            hoval_gw.interpret_message(bytes.fromhex('42 01 00 00 00 00 FE')),
            ((1, 0, 0), 'Heating 1 - Mixed flow temperature', 25.4),
        )
        self.assertEqual(
            hoval_gw.interpret_message(bytes.fromhex('42 02 00 01 F9 53 65 6D 61 69 6E 65 20 31')),
            ((2, 0, 505), 'Hot water 1 - Current week programme name', 'Semaine 1'),
        )
        self.assertEqual(
            hoval_gw.interpret_message(bytes.fromhex('42 01 02 1B A4 00')),
            ((1, 2, 7076), 'Unknown datapoint 1/2/7076', '00'),
        )
        self.assertEqual(
            hoval_gw.interpret_message(bytes.fromhex('42 FF FE 12 34 DE AD')),
            ((255, 254, 4660), 'Unknown datapoint 255/254/4660', 'DEAD'),
        )


if __name__ == '__main__':
    unittest.main()
