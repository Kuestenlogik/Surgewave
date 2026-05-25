using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

internal static class SchemaJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}
