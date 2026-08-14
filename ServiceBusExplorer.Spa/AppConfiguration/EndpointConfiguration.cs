using Azure.Messaging.ServiceBus;
using ServiceBusExplorer.ApplicationHelpers;

namespace ServiceBusExplorer.AppConfiguration;

public static class EndpointConfiguration
{
    private const int MaxMessages = 100;

    public static WebApplication ConfigureEndpoints(this WebApplication app)
    {
        // Returns the quick-load connection presets defined in connections.json (if any).
        app.MapGet(
            "/api/connections",
            async (IWebHostEnvironment env) =>
            {
                var presets = await ConnectionPresetsHelper.LoadAsync(env.ContentRootPath);

                return Results.Ok(presets);
            }
        );

        // Peeks messages from the active queue and its dead-letter sub-queue.
        app.MapGet(
            "/api/messages",
            async (HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);

                var active = await ServiceBusHelper.PeekAsync(
                    client,
                    queueName,
                    SubQueue.None,
                    MaxMessages
                );

                var deadLetter = await ServiceBusHelper.PeekAsync(
                    client,
                    queueName,
                    SubQueue.DeadLetter,
                    MaxMessages
                );

                var counts = await ServiceBusHelper.GetCountsAsync(connectionString, queueName);

                return Results.Ok(
                    new
                    {
                        active,
                        deadLetter,
                        counts,
                    }
                );
            }
        );

        // Deletes a single active message by sequence number.
        app.MapDelete(
            "/api/active/{sequenceNumber:long}",
            async (long sequenceNumber, HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);

                return await ServiceBusHelper.ProcessMessageAsync(
                    client,
                    queueName,
                    SubQueue.None,
                    sequenceNumber,
                    (msg, receiver, _) => receiver.CompleteMessageAsync(msg)
                );
            }
        );

        // Deletes a single dead-letter message by sequence number.
        app.MapDelete(
            "/api/deadletter/{sequenceNumber:long}",
            async (long sequenceNumber, HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);

                return await ServiceBusHelper.ProcessMessageAsync(
                    client,
                    queueName,
                    SubQueue.DeadLetter,
                    sequenceNumber,
                    (msg, receiver, _) => receiver.CompleteMessageAsync(msg)
                );
            }
        );

        // Deletes all active messages from the queue.
        app.MapDelete(
            "/api/active",
            async (HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);

                return await ServiceBusHelper.PurgeAsync(client, queueName, SubQueue.None);
            }
        );

        // Deletes all dead-letter messages from the queue.
        app.MapDelete(
            "/api/deadletter",
            async (HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);

                return await ServiceBusHelper.PurgeAsync(client, queueName, SubQueue.DeadLetter);
            }
        );

        // Resubmits a dead-letter message back onto the active queue, then removes it from the DLQ.
        app.MapPost(
            "/api/deadletter/{sequenceNumber:long}/resubmit",
            async (long sequenceNumber, HttpRequest request) =>
            {
                if (
                    !ConfigHelper.TryGetConfig(
                        request,
                        out var connectionString,
                        out var queueName,
                        out var error
                    )
                )
                {
                    return Results.Problem(error);
                }

                await using var client = new ServiceBusClient(connectionString);
                await using var sender = client.CreateSender(queueName);

                return await ServiceBusHelper.ProcessMessageAsync(
                    client,
                    queueName,
                    SubQueue.DeadLetter,
                    sequenceNumber,
                    async (msg, receiver, _) =>
                    {
                        var clone = new ServiceBusMessage(msg.Body)
                        {
                            ContentType = msg.ContentType,
                            Subject = msg.Subject,
                            CorrelationId = msg.CorrelationId,
                            MessageId = msg.MessageId,
                            To = msg.To,
                            ReplyTo = msg.ReplyTo,
                            ReplyToSessionId = msg.ReplyToSessionId,
                            SessionId = msg.SessionId,
                        };

                        foreach (var kv in msg.ApplicationProperties)
                        {
                            clone.ApplicationProperties[kv.Key] = kv.Value;
                        }

                        await sender.SendMessageAsync(clone);
                        await receiver.CompleteMessageAsync(msg);
                    }
                );
            }
        );

        return app;
    }
}
