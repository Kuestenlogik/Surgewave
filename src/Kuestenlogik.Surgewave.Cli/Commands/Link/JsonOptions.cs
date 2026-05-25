using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}
