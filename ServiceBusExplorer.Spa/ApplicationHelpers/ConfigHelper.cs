namespace ServiceBusExplorer.ApplicationHelpers;

public static class ConfigHelper
{
    private const string ConnectionStringHeader = "X-ServiceBus-ConnectionString";
    private const string QueueNameHeader = "X-ServiceBus-Queue";

    // Reads the Service Bus connection details from request headers supplied by the client.
    // The connection string is never stored on the server or in configuration; it must be
    // provided by the user on every request.
    public static bool TryGetConfig(
        HttpRequest request,
        out string connectionString,
        out string queueName,
        out string error
    )
    {
        connectionString = request.Headers[ConnectionStringHeader].ToString();
        queueName = request.Headers[QueueNameHeader].ToString();
        error = "";

        if (!string.IsNullOrWhiteSpace(connectionString) && !string.IsNullOrWhiteSpace(queueName))
        {
            return true;
        }

        error = "A Service Bus connection string and queue name must be provided.";

        return false;
    }
}
