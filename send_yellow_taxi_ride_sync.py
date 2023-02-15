import time
from time import sleep
import csv
import json

from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://wortellsmartlearning.servicebus.windows.net/;SharedAccessKeyName=sendKey;SharedAccessKey=GaOxm+5uLwBQ1BJEznRg0w8SA7bghL/Cy+AEhIywBlc="
EVENT_HUB_NAME = "myeh"

start_time = time.time()

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

with open('data/rides.csv') as file:
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        # key = {"vendorId": int(row[0])}
        message = {"vendorId": int(row[0]), "passenger_count": int(row[3]), "trip_distance": float(row[4]), "payment_type": int(row[9]), "total_amount": float(row[16])}
        
        event_data_batch = producer.create_batch()

        event_data_batch.add(EventData(json.dumps(message)))
        producer.send_batch(event_data_batch)
        print("producing")
        sleep(1)