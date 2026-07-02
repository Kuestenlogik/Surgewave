using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Describe a replication flow (surgewave mirror describe)
/// </summary>
public class DescribeMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow" };

    public DescribeMirrorCommand() : base("describe", "Show details of a replication flow")
    {
        Aliases.Add("show");
        Arguments.Add(_nameArgument);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArgument);
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Fetching details for replication flow '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var info = await client.Connect.GetConnectorAsync($"{name}-source", ct);
            if (info == null)
            {
                WriteError($"Replication flow '{name}' not found (no connector '{name}-source')");
                return 1;
            }

            var config = info.Config;

            // Companion connectors are optional — check what actually exists
            var checkpointStatus = await client.Connect.GetConnectorStatusAsync($"{name}-checkpoint", ct);
            var heartbeatStatus = await client.Connect.GetConnectorStatusAsync($"{name}-heartbeat", ct);

            var details = new
            {
                Name = name,
                Status = info.State.ToUpperInvariant(),
                SourceCluster = new
                {
                    Alias = config.GetValueOrDefault("source.cluster.alias", "unknown"),
                    BootstrapServers = config.GetValueOrDefault("source.bootstrap.servers", "unknown")
                },
                TargetCluster = new
                {
                    Alias = config.GetValueOrDefault("target.cluster.alias", "unknown"),
                    BootstrapServers = config.GetValueOrDefault("target.bootstrap.servers", "unknown")
                },
                TopicsPattern = config.GetValueOrDefault("topics", ".*"),
                TopicsWhitelist = config.GetValueOrDefault("topics.whitelist", ""),
                TopicsBlacklist = config.GetValueOrDefault("topics.blacklist", ""),
                Tasks = info.Tasks.Count,
                Connectors = new
                {
                    Source = info.State.ToUpperInvariant(),
                    Checkpoint = checkpointStatus?.State.ToUpperInvariant(),
                    Heartbeat = heartbeatStatus?.State.ToUpperInvariant()
                }
            };

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(details, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"{details.Name}\t{details.Status}\t{details.SourceCluster.Alias}\t{details.TargetCluster.Alias}\t{details.TopicsPattern}\t{details.Tasks}");
                Console.WriteLine($"  Checkpoint\t{details.Connectors.Checkpoint ?? "not deployed"}");
                Console.WriteLine($"  Heartbeat\t{details.Connectors.Heartbeat ?? "not deployed"}");
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Replication Flow: {name}[/]\n");

                var statusColor = details.Status switch
                {
                    "RUNNING" => "green",
                    "PAUSED" => "yellow",
                    "FAILED" => "red",
                    _ => "dim"
                };

                var table = new Table();
                table.AddColumn("Property");
                table.AddColumn("Value");
                table.Border = TableBorder.Rounded;

                table.AddRow("Status", $"[{statusColor}]{details.Status}[/]");
                table.AddRow("Source Cluster", $"{details.SourceCluster.Alias} ({details.SourceCluster.BootstrapServers})");
                table.AddRow("Target Cluster", $"{details.TargetCluster.Alias} ({details.TargetCluster.BootstrapServers})");
                table.AddRow("Topics Pattern", details.TopicsPattern);

                if (!string.IsNullOrEmpty(details.TopicsWhitelist))
                    table.AddRow("Topics Whitelist", details.TopicsWhitelist);
                if (!string.IsNullOrEmpty(details.TopicsBlacklist))
                    table.AddRow("Topics Blacklist", details.TopicsBlacklist);

                table.AddRow("Tasks", details.Tasks.ToString());

                AnsiConsole.Write(table);

                AnsiConsole.MarkupLine("\n[bold]Connectors[/]");
                var connectorTable = new Table();
                connectorTable.AddColumn("Connector");
                connectorTable.AddColumn("State");
                connectorTable.Border = TableBorder.Simple;

                connectorTable.AddRow($"{name}-source", $"[{statusColor}]{details.Connectors.Source}[/]");
                connectorTable.AddRow($"{name}-checkpoint", details.Connectors.Checkpoint ?? "[dim]not deployed[/]");
                connectorTable.AddRow($"{name}-heartbeat", details.Connectors.Heartbeat ?? "[dim]not deployed[/]");

                AnsiConsole.Write(connectorTable);

                AnsiConsole.MarkupLine("\n[dim]Use 'surgewave mirror status' for per-task states.[/]");
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
