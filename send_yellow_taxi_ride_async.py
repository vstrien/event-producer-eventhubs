import asyncio
import time
from time import sleep
import csv
import json

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://wortellsmartlearning.servicebus.windows.net/;SharedAccessKeyName=sendKey;SharedAccessKey=GaOxm+5uLwBQ1BJEznRg0w8SA7bghL/Cy+AEhIywBlc="
EVENT_HUB_NAME = "myeh"

start_time = time.time()

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

async def run():
    with open('data/rides.csv') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            # key = {"vendorId": int(row[0])}
            message = {"vendorId": int(row[0]), "passenger_count": int(row[3]), "trip_distance": float(row[4]), "payment_type": int(row[9]), "total_amount": float(row[16])}
            
            event_data_batch = await producer.create_batch()

            event_data_batch.add(EventData(json.dumps(message)))
            await producer.send_batch(event_data_batch)
            print("producing")
            sleep(1)
# producer:
#         # Create a batch.
#         event_data_batch = await producer.create_batch()

#         # Add events to the batch.
#         event_data_batch.add(EventData("{'key': 'event', 'value': 'First event'}"))
#         event_data_batch.add(EventData("{'key': 'event', 'value': 'Second event'}"))
#         event_data_batch.add(EventData("{'key': 'event', 'value': 'Third event'}"))

#         # Send the batch of events to the event hub.
#         await producer.send_batch(event_data_batch)

# asyncio.run(run())