namespace ServiceBusExplorer.Models;

public record MessageDto(
    string MessageId,
    string? Subject,
    DateTimeOffset EnqueuedTime,
    long SequenceNumber,
    int DeliveryCount,
    string? ContentType,
    string? DeadLetterReason,
    string Body
);
