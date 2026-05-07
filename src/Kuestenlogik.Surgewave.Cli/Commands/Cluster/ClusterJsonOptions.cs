using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

internal static class ClusterJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}
