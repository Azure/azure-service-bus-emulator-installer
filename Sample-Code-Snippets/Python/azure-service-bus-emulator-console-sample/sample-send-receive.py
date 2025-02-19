# Install the Azure Service Bus SDK for Python using following command prior to running the sample. Versions >=7.14.0 support Emulator.
# pip install azure-servicebus==7.14.0 

from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONNECTION_STR = "Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  
QUEUE_NAME = "queue.1" 

def send_message(sender):
    message = ServiceBusMessage("Hello, Azure Service Bus!")
    sender.send_messages(message)
    print("Message sent:", message)

def receive_messages(receiver):
    with receiver:
        for msg in receiver.receive_messages(max_message_count=1, max_wait_time=5):
            print("Received message:", str(msg))
            receiver.complete_message(msg)

def main():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            send_message(sender)

        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
        with receiver:
            receive_messages(receiver)

if __name__ == "__main__":
    main()