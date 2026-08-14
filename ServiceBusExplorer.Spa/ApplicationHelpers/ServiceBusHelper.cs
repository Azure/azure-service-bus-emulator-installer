using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using ServiceBusExplorer.Models;

namespace ServiceBusExplorer.ApplicationHelpers;

public static class ServiceBusHelper
{
    private const int MaxScan = 500;

    // Returns the true message counts for the queue (active and dead-letter) using the
    // management API, independent of how many messages are peeked for display.
    // Requires the "Manage" claim on the SAS policy; returns null if unavailable so that
    // callers with Listen/Send-only connection strings can still load messages.
    public static async Task<QueueCountsDto?> GetCountsAsync(
        string connectionString,
        string queueName
    )
    {
        try
        {
            var admin = new ServiceBusAdministrationClient(connectionString);

            var properties = await admin.GetQueueRuntimePropertiesAsync(queueName);

            return new QueueCountsDto(
                properties.Value.ActiveMessageCount,
                properties.Value.DeadLetterMessageCount
            );
        }
        catch
        {
            // Connection string likely lacks the "Manage" claim; fall back to peek counts.
            return null;
        }
    }

    public static async Task<List<MessageDto>> PeekAsync(
        ServiceBusClient client,
        string queueName,
        SubQueue subQueue,
        int max
    )
    {
        await using var receiver = client.CreateReceiver(
            queueName,
            new ServiceBusReceiverOptions { SubQueue = subQueue }
        );

        var messages = await receiver.PeekMessagesAsync(max);

        return
        [
            .. messages.Select(m => new MessageDto(
                m.MessageId,
                m.Subject,
                m.EnqueuedTime,
                m.SequenceNumber,
                m.DeliveryCount,
                m.ContentType,
                m.DeadLetterReason,
                m.Body.ToString()
            )),
        ];
    }

    // Deletes every message from the queue (or its dead-letter sub-queue) by receiving them in
    // ReceiveAndDelete mode until an empty batch is returned. Returns the number of messages removed.
    public static async Task<IResult> PurgeAsync(
        ServiceBusClient client,
        string queueName,
        SubQueue subQueue
    )
    {
        await using var receiver = client.CreateReceiver(
            queueName,
            new ServiceBusReceiverOptions
            {
                SubQueue = subQueue,
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            }
        );

        var deleted = 0;

        while (true)
        {
            var batch = await receiver.ReceiveMessagesAsync(
                maxMessages: 100,
                TimeSpan.FromSeconds(5)
            );

            if (batch.Count == 0)
            {
                break;
            }

            deleted += batch.Count;
        }

        return Results.Ok(new { success = true, deleted });
    }

    // Receives messages in PeekLock mode until the target sequence number is found, runs the
    // supplied action on it, then abandons all other locked messages so they return to the queue.
    public static async Task<IResult> ProcessMessageAsync(
        ServiceBusClient client,
        string queueName,
        SubQueue subQueue,
        long sequenceNumber,
        Func<ServiceBusReceivedMessage, ServiceBusReceiver, ServiceBusClient, Task> action
    )
    {
        await using var receiver = client.CreateReceiver(
            queueName,
            new ServiceBusReceiverOptions
            {
                SubQueue = subQueue,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            }
        );

        var held = new List<ServiceBusReceivedMessage>();
        ServiceBusReceivedMessage? target = null;
        var scanned = 0;

        try
        {
            while ((scanned < MaxScan) && target is null)
            {
                var batch = await receiver.ReceiveMessagesAsync(
                    maxMessages: 20,
                    TimeSpan.FromSeconds(5)
                );

                if (batch.Count == 0)
                {
                    break;
                }

                foreach (var msg in batch)
                {
                    scanned++;

                    if (msg.SequenceNumber == sequenceNumber)
                    {
                        target = msg;

                        break;
                    }

                    held.Add(msg);
                }
            }

            if (target is null)
            {
                return Results.NotFound(
                    new
                    {
                        message = "Message not found. It may have already been consumed or moved.",
                    }
                );
            }

            await action(target, receiver, client);

            return Results.Ok(new { success = true });
        }
        finally
        {
            foreach (var msg in held)
            {
                try
                {
                    await receiver.AbandonMessageAsync(msg);
                }
                catch
                {
                    /* lock may have expired; ignore */
                }
            }
        }
    }
}
