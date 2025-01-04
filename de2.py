pip install paho

import streamlit as st
import asyncio
from mqttcode import MQTTConnection
from sqlalchemy import create_engine, text

# Database configuration (same as before)
DB_USERNAME = "postgres"
DB_PASSWORD = "Ganges#!2024"
DB_HOST = "ganges-db.cgbxtsvwnmqu.ap-south-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "dev"

# Database connection function (same as before)
def get_database_connection():
    try:
        engine = create_engine(
            f"postgresql+pg8000://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Fetch functions (same as before)
def fetch_all_gateway_ids(engine):
    try:
        query = text("SELECT gateway_did, name FROM site WHERE deleted = 'False' ORDER BY id ASC LIMIT 100")
        with engine.connect() as conn:
            result = conn.execute(query)
            return [(row[0], row[1]) for row in result.fetchall()]
    except Exception as e:
        st.error(f"Failed to fetch Gateway IDs: {e}")
        return []



def fetch_nodes_by_gateway(engine, gateway_id):
    try:
        query = text("""
            SELECT robot.name, cleaner_did
            FROM robot
            LEFT JOIN site
            ON robot.site_id = site.id
            WHERE site.gateway_did = :gateway_id AND robot.is_repeater = 'False'
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {"gateway_id": gateway_id})
            return [{"name": row[0], "cleaner_did": row[1]} for row in result.fetchall()]
    except Exception as e:
        st.error(f"Failed to fetch Node IDs for Gateway ID {gateway_id}: {e}")
        return []

# Main Streamlit application
def main():
    st.title("MQTT Command Dashboard")

    # Database connection
    engine = get_database_connection()
    if not engine:
        st.stop()

    # Fetch Gateway IDs and display dropdown
    gateway_ids = fetch_all_gateway_ids(engine)
    if not gateway_ids:
        st.error("No Gateway IDs found.")
        st.stop()
    gateways = fetch_all_gateway_ids(engine)
    gateway_mapping = {f"{name} ({gateway_id})": gateway_id for gateway_id, name in gateways}
    selected_gateway_display = st.selectbox("Select Gateway", list(gateway_mapping.keys()))
    gateway_id = gateway_mapping[selected_gateway_display]


    # Fetch Nodes for the selected Gateway and display dropdown
    nodes = fetch_nodes_by_gateway(engine, gateway_id)
    if not nodes:
        st.error(f"No nodes found for Gateway ID {gateway_id}.")
        st.stop()

    # Display node information in the dropdown
    node_display = [f"{node['name']} (Cleaner DID: {node['cleaner_did']})" for node in nodes]
    selected_node = st.selectbox("Select Node", node_display)

    # Extract the selected node's details
    selected_node_index = node_display.index(selected_node)
    selected_node_details = nodes[selected_node_index]

    # Extract cleaner_did (node_id) from the selected node
    node_id = selected_node_details["cleaner_did"]

# Use node_id in further logic
    st.write(f"Selected Node ID: {node_id}")


    # Initialize MQTTConnection instance
    if 'mqtt_client' not in st.session_state:
        st.session_state.mqtt_client = MQTTConnection()

    mqtt_client = st.session_state.mqtt_client
    mqtt_client.gateway_id = gateway_id  # Set selected Gateway ID in MQTT

    # Connect to MQTT broker
    if st.button("Connect to MQTT Broker"):
        if mqtt_client.connect():
            st.success("Connected to MQTT broker.")
        else:
            st.error("Failed to connect to MQTT broker.")

    # Display the commands
    st.subheader("Commands to Send")
    st.write("The following commands will be sent to the selected node:")
    st.json(mqtt_client.commands)  # Display the list of commands

    # Send button to trigger command sending
    if st.button("Send Commands"):
        async def send_commands():
            await mqtt_client.send_commands_and_collect(node_id)
            st.success("Commands sent and responses collected successfully.")

        asyncio.run(send_commands())

    # Disconnect MQTT
    if st.button("Disconnect"):
        mqtt_client.Client.loop_stop()
        mqtt_client.Client.disconnect()
        st.success("Disconnected from MQTT broker.")

if __name__ == "__main__":
    main()
