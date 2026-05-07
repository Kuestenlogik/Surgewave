using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}
