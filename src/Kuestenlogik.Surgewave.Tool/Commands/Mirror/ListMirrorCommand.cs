using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// List all replication flows (surgewave mirror list)
/// </summary>
public class ListMirrorCommand : CommandBase
{
    public ListMirrorCommand() : base("list", "List all replication flows")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var connectorNames = await client.Connect.ListAsync(ct);
            var mirrors = new List<(string Name, string SourceCluster, string TargetCluster, string Status, int Tasks)>();

            // Mirror flows are identified by the '-source' suffix and the MirrorSourceConnector class
            foreach (var connectorName in connectorNames.Where(n => n.EndsWith("-source", StringComparison.Ordinal)).OrderBy(n => n))
            {
                var info = await client.Connect.GetConnectorAsync(connectorName, ct);
                if (info == null)
                    continue;

                if (!info.Config.TryGetValue("connector.class", out var connectorClass) ||
                    !connectorClass.Contains("MirrorSourceConnector", StringComparison.Ordinal))
                    continue;

                mirrors.Add((
                    connectorName[..^"-source".Length],
                    info.Config.GetValueOrDefault("source.cluster.alias", "unknown"),
                    info.Config.GetValueOrDefault("target.cluster.alias", "unknown"),
                    info.State.ToUpperInvariant(),
                    info.Tasks.Count));
            }

            if (format == OutputFormat.Json)
            {
                var output = mirrors
                    .Select(m => new { m.Name, m.SourceCluster, m.TargetCluster, m.Status, m.Tasks })
                    .ToList();
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var mirror in mirrors)
                {
                    Console.WriteLine($"{mirror.Name}\t{mirror.SourceCluster}->{mirror.TargetCluster}\t{mirror.Status}");
                }
            }
            else
            {
                if (mirrors.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No replication flows found.[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Name");
                table.AddColumn("Source");
                table.AddColumn("Target");
                table.AddColumn("Status");
                table.AddColumn("Tasks");

                foreach (var mirror in mirrors)
                {
                    var statusColor = mirror.Status switch
                    {
                        "RUNNING" => "green",
                        "PAUSED" => "yellow",
                        "FAILED" => "red",
                        _ => "dim"
                    };
                    table.AddRow(
                        mirror.Name,
                        mirror.SourceCluster,
                        mirror.TargetCluster,
                        $"[{statusColor}]{mirror.Status}[/]",
                        mirror.Tasks.ToString()
                    );
                }

                AnsiConsole.Write(table);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
