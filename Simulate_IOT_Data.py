### This is used to create a simulated data stream to Azure IoT Hub for a drilling rig.

import pandas as pd
import time
from azure.iot.device import IoTHubDeviceClient, Message

# Load your data into a Pandas DataFrame
data = pd.read_csv('Data/Data_for_ROP_optimization.csv')
data = data.head()

# Azure IoT hub connection string
connection_string = ''
client = IoTHubDeviceClient.create_from_connection_string(connection_string)

# Function to send data to IoT hub
def send_data_to_iot_hub(row):
    message = Message(str(row.to_json()))
    print("Sending message: ", message)
    client.send_message(message)
    print("Message successfully sent")

# Loop through each row in the DataFrame and send it
for index, row in data.iterrows():
    send_data_to_iot_hub(row)
    time.sleep(1)  # Wait for 1 second

# Finally, shut down the client
client.shutdown()
