import logging
import can
import json

logging.basicConfig(level=logging.DEBUG)

# To use that program, first install dependencies:
# pip3 install tk  python-can-remote python-can
# Then start the remote server on the PI:
# Start on PI: python -m can_remote --interface=socketcan
# Start on PC: python ui.py

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

REQUEST = 0x40
ANSWER = 0x42
SET_REQUEST = 0x46
pending_msg={}
devices={}
def interpret_message(data):
    if data[0] == ANSWER: # Answer from request
        # Data point
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        ref = "%05d" % (datapoint)
        ref = ref[0:2]+"-"+ref[2:]
        ref = f"{function_group:02d}-{function_number:02d} {ref}"
        if idp in data_idx:
            point = data_idx[idp]
            out = convert_data(data[5:], point)
            if out:
                return (point[0], out, ref)
        else:
            logging.error("No known point found for (%d,%d,%d), len %d", function_group, function_number, datapoint, len(data))
            return (ref, data[5:].hex(), ref)
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
        function_group = data[1]
        function_number = data[2]
        datapoint = int.from_bytes(data[3:5], byteorder='big', signed=False)
        idp = (function_group, function_number, datapoint)
        if idp in data_idx:
            out = convert_data(data[5:], data_idx[idp])
            logging.debug("Unknown op code: 0x%02x  %d %s %s",  data[0],idp, data_idx[idp], out)
        else:
            logging.debug("Unknown op code: 0x%02x (%d, %d)", data[0],data[2],data[1])

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


def parse(arbitration_id, data):
    id = parse_can_id(arbitration_id)
    logging.info("%x %s",arbitration_id, data.hex())
    msg_id = id[0] >> 8
    if id[1] != 0x0f or id[2] != 0xff:
        # Message to a device with device ID / device type? Not sure
        if (id[1],id[2]) not in devices:
            logging.info("Message to device type / id: %02x,%02x", id[1], id[2])
            devices[(id[1],id[2])] = True
        return None

    if msg_id == 0x1f:
        if len(data) >= 2:
            # Start of a message

            # Number of CAN message we need to get to rebuild this message, 0 is none.
            msg_len = data[0] >> 3
            if msg_len == 0:
                try:
                    return interpret_message(data[1:])
                except:
                    logging.exception(data)
                    return None
            else:
                msg_header = data[1]
                pending_msg[msg_header] = {
                    "data": data[2:],
                    "nb_remaining": msg_len - 1
                }
        else:
            logging.error("Message too small")
    else:
        # Message part
        msg_header = data[0]
        # Check if we are expecting it
        if msg_header in pending_msg:
            pending_msg[msg_header]["data"] = pending_msg[msg_header]["data"] + data[1:]
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


import tkinter as tk
from tkinter import ttk
import asyncio
from async_tkinter_loop import async_handler, async_mainloop

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Continuously Refreshing Table")

         # Create a table using Treeview
        self.tree = ttk.Treeview(root, columns=("Name", "Value"), show="headings")
        self.tree.heading("Name", text="Name")
        self.tree.heading("Value", text="Value")
        self.tree.grid(row=0, column=0, sticky="nsew", columnspan=2)

        # Scrollbar for the table
        self.scrollbar = ttk.Scrollbar(root, orient="vertical", command=self.tree.yview)
        self.scrollbar.grid(row=0, column=2, sticky="ns")
        self.tree.configure(yscrollcommand=self.scrollbar.set)

        # Button to start the continuous refresh
        self.start_button = ttk.Button(root, text="Start", command=self.start_refresh)
        self.start_button.grid(row=1, column=0, columnspan=3, pady=20, sticky="ew")

        # Button to stop the continuous refresh
        self.stop_button = ttk.Button(root, text="Stop", command=self.stop_refresh)
        self.stop_button.grid(row=2, column=0, columnspan=3, pady=20, sticky="ew")


        # Text fields and send button
        self.text_field1 = ttk.Entry(root)
        self.text_field1.grid(row=3, column=0, padx=10, pady=10, sticky="ew")

        self.text_field2 = ttk.Entry(root)
        self.text_field2.grid(row=3, column=1, padx=10, pady=10, sticky="ew")

        self.send_button = ttk.Button(root, text="Send", command=self.send)
        self.send_button.grid(row=3, column=2, padx=10, pady=10)

        self.refreshing = False

        # Configure grid weights
        root.grid_rowconfigure(0, weight=1)
        root.grid_columnconfigure(0, weight=1)
        root.grid_columnconfigure(1, weight=1)

        self.start_id=1
        self.to_send = None

        self.data={}

    def send(self):
        # Print the values of the text fields
        #print(self.text_field1.get(), self.text_field2.get())
        arb_id = self.start_id % 0x10
        arb_id = (0x1F0 + arb_id) << 16
        arb_id += 0x0801 # This is the fixed address?
        msg = can.Message(arbitration_id=arb_id,
                        data=list(bytes.fromhex(self.text_field2.get())),
                        is_extended_id=True)
        self.to_send = msg

    def start_refresh(self):
        self.refreshing = True
        self.can0 = can.Bus('ws://192.168.3.19:54701/',
              bustype='remote',
              receive_own_messages=True)


        self.reader = can.AsyncBufferedReader()
        self.notifier = can.Notifier(self.can0, [self.reader], loop=asyncio.get_event_loop())
        self.refresh_table()

    def stop_refresh(self):
        if self.refreshing:
            self.refreshing = False
            self.reader = None
            self.notifier.stop()
            self.notifier = None
            self.can0.shutdown()
            self.can0 = None

    def refresh(self):
        # Clear existing items
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Simulating multiple calls to parse(buffer)
        for (n,v) in self.data.items():
            self.tree.insert("", "end", values=(n, v))

    @async_handler
    async def refresh_table(self):
        if not self.refreshing:
            return
        
        while self.refreshing:
            msg = await self.reader.get_message()
            if not self.refreshing:
                return
            parsed = parse(msg.arbitration_id, msg.data)
            if parsed:
                name, value, b = parsed
                self.data[name + f" ({b})"] = value
                logging.info(parsed)
                self.refresh()
            if self.to_send:
                self.can0.send(self.to_send)
                self.to_send = None


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    async_mainloop(root)
