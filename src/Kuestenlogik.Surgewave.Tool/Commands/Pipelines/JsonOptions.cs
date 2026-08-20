using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Indented = new(JsonSerializerDefaults.Web) { WriteIndented = true };
}
