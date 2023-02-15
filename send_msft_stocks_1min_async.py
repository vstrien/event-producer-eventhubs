import asyncio
import time
from time import sleep
import csv
import json
from datetime import datetime
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://wortellsmartlearning.servicebus.windows.net/;SharedAccessKeyName=sendKey;SharedAccessKey=GaOxm+5uLwBQ1BJEznRg0w8SA7bghL/Cy+AEhIywBlc="
EVENT_HUB_NAME = "myeh"

start_time = time.time()

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

async def run():
    with open('data/msft.csv') as file:
        csvreader = csv.reader(file)
        timetowait = 0
        currenttime = datetime.now() # Initially we need just any date to start with (act as if the previous trade was "currenttime"). Note that this is due to the nature of replaying the history.
        for row in csvreader:
            
            # Determine how long to wait before outputting the new trade
            prevtime = currenttime
            currenttime = datetime.fromisoformat(row[0])
            if prevtime > currenttime:
                # This is the first row
                timetowait = 0
            else:
                # Wait the number of seconds between the previous and the current trade.
                # Could be more precise by using a timer instead of a sleep function, but this will do for demonstration purposes.
                timetowait = (currenttime - prevtime).total_seconds()
            sleep(timetowait)

            # Construct the message we will send to event hubs
            message = {"dateTime": str(row[0]), "Open": float(row[1]), "High": float(row[2]), "Low": float(row[3]), "Close": float(row[4]), "Volume": int(row[5])}
            event_data_batch = await producer.create_batch()
            event_data_batch.add(EventData(json.dumps(message)))
            await producer.send_batch(event_data_batch)

            print("producing ", message)

            
            
asyncio.run(run())