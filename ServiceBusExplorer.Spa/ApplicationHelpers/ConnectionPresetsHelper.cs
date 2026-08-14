using System.Text.Json;
using ServiceBusExplorer.Models;

namespace ServiceBusExplorer.ApplicationHelpers;

// Loads quick-load connection presets from a connections.json file located in the
// application content root. The file is optional and can be replaced at runtime
// (e.g. mounted as a Docker volume) without rebuilding the image.
public static class ConnectionPresetsHelper
{
    private const string FileName = "connections.json";

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public static async Task<IReadOnlyList<ConnectionPreset>> LoadAsync(string contentRootPath)
    {
        var path = Path.Combine(contentRootPath, FileName);

        if (!File.Exists(path))
        {
            return [];
        }

        try
        {
            await using var stream = File.OpenRead(path);

            var file = await JsonSerializer.DeserializeAsync<ConnectionPresetFile>(
                stream,
                SerializerOptions
            );

            var connections = file?.Connections;

            if (connections is null)
            {
                return [];
            }

            return
            [
                .. connections.Where(c =>
                    !string.IsNullOrWhiteSpace(c.Name)
                    && !string.IsNullOrWhiteSpace(c.ConnectionString)
                ),
            ];
        }
        catch
        {
            // A malformed presets file should never take the app down; just show none.
            return [];
        }
    }
}
