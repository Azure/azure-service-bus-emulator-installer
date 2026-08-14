namespace ServiceBusExplorer.Models;

// A named, quick-load Service Bus connection defined in connections.json.
public record ConnectionPreset(string Name, string ConnectionString, string Queue);

// Root object of connections.json.
public record ConnectionPresetFile(List<ConnectionPreset>? Connections);
