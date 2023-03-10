import asyncio

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://wortellsmartlearning.servicebus.windows.net/;SharedAccessKeyName=sendKey;SharedAccessKey=GaOxm+5uLwBQ1BJEznRg0w8SA7bghL/Cy+AEhIywBlc="
EVENT_HUB_NAME = "myeh"

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData("{'key': 'event', 'value': 'First event'}"))
        event_data_batch.add(EventData("{'key': 'event', 'value': 'Second event'}"))
        event_data_batch.add(EventData("{'key': 'event', 'value': 'Third event'}"))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

asyncio.run(run())