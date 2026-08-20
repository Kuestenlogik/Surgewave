using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines.Serialization;

/// <summary>
/// JSON conventions for pipeline export files: camelCase property names and indented output,
/// byte-compatible with the Control UI's export dialog and the broker's import endpoint.
/// </summary>
public static class PipelineJson
{
    /// <summary>Options for reading/writing pipeline export JSON.</summary>
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
    };

    /// <summary>Serializes an export to indented camelCase JSON.</summary>
    public static string Write(PipelineExportFormat export)
    {
        ArgumentNullException.ThrowIfNull(export);
        return JsonSerializer.Serialize(export, Options);
    }

    /// <summary>Deserializes pipeline export JSON.</summary>
    public static PipelineExportFormat Read(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        return JsonSerializer.Deserialize<PipelineExportFormat>(json, Options)
            ?? throw new JsonException("The document does not contain a pipeline export.");
    }

    /// <summary>Reads a pipeline export from a file.</summary>
    public static PipelineExportFormat ReadFile(string path)
    {
        return Read(File.ReadAllText(path));
    }
}
