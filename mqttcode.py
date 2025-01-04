
import paho.mqtt.client as mqtt
import ssl
import json
import csv
import os
import asyncio
from sqlalchemy import create_engine, text


class MQTTConnection:
    def __init__(self, config_file='C:/Users/Kansal/Downloads/command.json'):
        self.gateway_id = ""
        self.auth = 1
        self.commands = []
        self.broker_address = "a184c9z7edjar9-ats.iot.ap-south-1.amazonaws.com"
        self.broker_port = 8883
        self.commands=self.load_commands(config_file)

        # Initialize MQTT client
        self.Client = mqtt.Client()
        self.Client.on_connect = self.on_connect
        self.Client.on_message = self.on_message
        self.Client.on_disconnect = self.on_disconnect

        # File paths for certificates and logs
        self.root_ca = "C:/Users/Kansal/Desktop/MQTT/root-CA.crt"
        self.cert_file = "C:/Users/Kansal/Desktop/MQTT/raspbery_iot_core.cert.pem"
        self.private_key = "C:/Users/Kansal/Desktop/MQTT/raspbery_iot_core.private.key"
        self.log_file = "C:/Users/Kansal/Downloads/mqtt_logs.csv"

        # Configure TLS settings
        self.Client.tls_set(
            ca_certs=self.root_ca,
            certfile=self.cert_file,
            keyfile=self.private_key,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )

        self.responses = {}  # Temporary storage for responses (keyed by DID)
        self.pending_commands = {}  # Track pending commands for each DID


    def on_connect(self, client, userdata, flags, reason_code):
        print(f"Connected: {reason_code}")
        topic_subscribed = f"GANGES/{self.gateway_id}/COMMAND/RESPONSE"
        self.Client.subscribe(topic_subscribed)
        print(f"Subscribed to topic: {topic_subscribed}")

    def on_message(self, client, userdata, msg):
        try:
            packet_rx = json.loads(msg.payload)
            did = packet_rx.get("DID")
            cmd_id = packet_rx.get("CMD")
            values = packet_rx.get("VALUES")

            if did and cmd_id and values:
                # Add response to temporary storage
                if did not in self.responses:
                    self.responses[did] = {}
                self.responses[did][cmd_id] = values

                # Decrease pending commands for this node
                if self.pending_commands.get(did, 0) > 0:
                    self.pending_commands[did] -= 1

            print(f"Received response for DID={did}, CMD={cmd_id}: {values}")

        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

            

    def on_disconnect(self, client, userdata, reason_code):
        print(f"Disconnected with reason: {reason_code}")
        print("Attempting Reconnection")
        try:
            self.Client.reconnect()
        except Exception as e:
            print(f"Reconnection Attempt Failed: {e}")

    def load_commands(self, config_file):
        """Load commands from a configuration file."""
        try:
            with open(config_file, "r") as file:
                config = json.load(file)
                return config.get("commands", [])
        except FileNotFoundError:
            print(f"Configuration file {config_file} not found. Using default commands.")
            return [("100C", "0"), ("100D", "0"), ("1019", "0")]  # Default commands
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file: {e}. Using default commands.")
            return [("100C", "0"), ("100D", "0"), ("1019", "0")]

    def on_all_commands_executed(self, node):
        print(f"All commands for node {node} have been executed.")

    def load_lookup(self, lookup_file='C:/Users/Kansal/Downloads/lookup.json'):
        """Load lookup data for human-readable command names."""
        try:
            with open(lookup_file, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"Lookup file {lookup_file} not found. Using raw command codes.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file: {e}. Using raw command codes.")
            return {}

    def save_to_csv(self, did, data, lookup_file='C:/Users/Kansal/Downloads/lookup.json'):
        file_exists = os.path.exists(self.log_file)
        
        # Load the lookup data
        lookup = self.load_lookup(lookup_file)
        
        # Map command codes to human-readable names
        allowed_keys = lookup.keys()
        headers = ["DID"] + [lookup.get(key, key) for key in allowed_keys]  # Use human-readable names if available
        
        # Filter and remap data for CSV
        filtered_data = {lookup.get(key, key): data[key] for key in allowed_keys if key in data}

        # Write to CSV
        if not file_exists:
            with open(self.log_file, mode='w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

        with open(self.log_file, mode='a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            
            # Ensure row matches headers
            row = {header: filtered_data.get(header, "") for header in headers if header != "DID"}
            row["DID"] = did
            writer.writerow(row)

        print(f"Saved row to CSV for DID={did}: {filtered_data}")



    async def send_commands_and_collect(self, node):
        """Send commands and collect responses."""
        self.responses[node] = {}
        self.pending_commands[node] = len(self.commands)

        for command in self.commands:
            cmd, value = command["cmd"], command["value"]
            await self._send_and_wait(node, cmd, value)

        # Wait for all responses
        while self.pending_commands[node] > 0:
            await asyncio.sleep(1)

        self.on_all_commands_executed(node)
        self.save_to_csv(node, self.responses[node])


    async def _send_and_wait(self, node, cmd, value):
        self.send_command(node, cmd, value)
        await asyncio.sleep(10)

    def send_command(self, node, cmd, value):
        command = {"DID": node, "CMD": cmd, "VALUES": value}
        topic = f"GANGES/{self.gateway_id}/COMMAND/GET"
        self.Client.publish(topic, json.dumps(command))
        print(f"Sent command: {command}")

    def connect(self):
        try:
            print("AWS Connection in process")
            self.Client.connect(self.broker_address, self.broker_port, keepalive=60)
            self.Client.loop_start()
            return True
        except Exception as e:
            print(f"Connection Failed: {e}")
            return False

    async def main(self):
        """
        Main function to run the command collection process.
        """
        # # gateway_ids = self.db_handler.fetch_all_gateway_ids()
        # if not gateway_ids:
        #     print("No Gateway IDs found.")
        #     return

        # for gateway_id in gateway_ids:
        #     print(f"Processing Gateway ID: {gateway_id}")
        #     node_ids = self.db_handler.fetch_nodes_by_gateway(gateway_id)
        #     if not node_ids:
        #         print(f"No Node IDs found for Gateway ID: {gateway_id}")
        #         continue

        #     for node in node_ids:
        #         await self.send_commands_and_collect(node)

# Initialize and run the MQTT client
mqtt_client = MQTTConnection()
if mqtt_client.connect():
    print("Successfully connected to AWS IoT")
    try:
        asyncio.run(mqtt_client.main())
    except KeyboardInterrupt:
        print("Shutting down...")
        mqtt_client.Client.loop_stop()
        mqtt_client.Client.disconnect()