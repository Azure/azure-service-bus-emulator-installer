using Azure.Messaging.ServiceBus;
using System.Text;

internal class Program
{
    private static string _connectionString = "Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Service Bus Emulator .NET Sample");

        //Case 1 : Send and Receive from a Queue
        Console.WriteLine("Running Case 1: Send Receive from a Queue");
        await PublishMessageToDefaultQueue();
        await ConsumeMessageFromDefaultQueue();

        //Case 2: Send and Receive from a Topic
        Console.WriteLine("Running Case 2: Send Receive from a Topic with 3 Subscription with varying correlation filters");
        await PublishMessageToDefaultTopic();
        await ConsumeMessageFromDefaultTopic();


        Console.WriteLine("Press enter key to exit.");
        Console.ReadLine();  
    }

    public static async Task PublishMessageToDefaultQueue() 
    {
        const int numOfMessagesPerBatch = 10;
        const int numOfBatches = 10;

        string queueName = "queue.1";

        var client = new ServiceBusClient(_connectionString);
        var sender = client.CreateSender(queueName);

        for (int i = 1; i <= numOfBatches; i++)
        {
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            for (int j = 1; j <= numOfMessagesPerBatch; j++)
            {
                messageBatch.TryAddMessage(new ServiceBusMessage($"Batch:{i}:Message:{j}"));
            }
            await sender.SendMessagesAsync(messageBatch);
        }

        await sender.DisposeAsync();
        await client.DisposeAsync();

        Console.WriteLine($"{numOfBatches} batches with {numOfMessagesPerBatch} messages per batch has been published to the queue.");
    }

    public static async Task ConsumeMessageFromDefaultQueue()
    {
        var connectionString = "Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        string queueName = "queue.1";

        var client = new ServiceBusClient(connectionString);

        ServiceBusReceiverOptions opt = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        };

        ServiceBusReceiver receiver = client.CreateReceiver(queueName, opt);

        while (true)
        {
            ServiceBusReceivedMessage message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            if (message != null)
            {
                // Process the message
                Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Body)}");

                // Complete the message to remove it from the queue
                await receiver.CompleteMessageAsync(message);
            }
            else
            {
                Console.WriteLine("No messages received.");
                break;
            }
        }

        Console.WriteLine("Done receiving.");

        await receiver.DisposeAsync();
        await client.DisposeAsync();
    }


    public static async Task PublishMessageToDefaultTopic()
    {
        var topicName = "topic.1";

        await using (var client = new ServiceBusClient(_connectionString))
        {
            ServiceBusSender sender = client.CreateSender(topicName);

            //First 50 message will goto Subscription 1 and Subscription 3 as per set filters in Config.json
            for (int i = 1; i <= 50; i++)
            {
                ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message number : {i}"))
                {
                    ContentType = "application/json"
                };

                await sender.SendMessageAsync(message);
            }

            //Next 50 message will goto Subscription 2 and Subscription 3 as per set filters  in Config.json

            for (int i = 51; i <= 100; i++)
            {
                ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message number : {i}"));
                message.ApplicationProperties.Add("prop1", "value1");

                await sender.SendMessageAsync(message);
            }
        }

        Console.WriteLine("Sent 100 messages to the topic.");
    }

    public static async Task ConsumeMessageFromDefaultTopic()
    {
        var topicName = "topic.1";
        await ConsumeMessageFromSubscription(topicName, "subscription.1");
        await ConsumeMessageFromSubscription(topicName, "subscription.2");
        await ConsumeMessageFromSubscription(topicName, "subscription.3");
    }

    private static async Task ConsumeMessageFromSubscription(string topicName,string subscriptionName)
    {
        Console.WriteLine($"Rcv_Sub {subscriptionName} Begin");

        //  Receive on Sub 1
        var client1 = new ServiceBusClient(_connectionString);
        var opt1 = new ServiceBusProcessorOptions();
        opt1.ReceiveMode = ServiceBusReceiveMode.PeekLock;
        var processor1 = client1.CreateProcessor(topicName, subscriptionName, opt1);

        processor1.ProcessMessageAsync += MessageHandler;
        processor1.ProcessErrorAsync += ErrorHandler;

        await processor1.StartProcessingAsync();

        await Task.Delay(TimeSpan.FromSeconds(5));

        await processor1.StopProcessingAsync();
        await processor1.DisposeAsync();
        await client1.DisposeAsync();
        Console.WriteLine($"Rcv_Sub {subscriptionName} End");
    }

    private static async Task MessageHandler(ProcessMessageEventArgs args)
    {
        string body = args.Message.Body.ToString();
        Console.WriteLine($"Received message: SequenceNumber:{args.Message.SequenceNumber} Body:{body}");
        await args.CompleteMessageAsync(args.Message);
    }

    private static Task ErrorHandler(ProcessErrorEventArgs args)
    {
        Console.WriteLine($"Message handler encountered an exception {args.Exception}.");
        return Task.CompletedTask;
    }

}
