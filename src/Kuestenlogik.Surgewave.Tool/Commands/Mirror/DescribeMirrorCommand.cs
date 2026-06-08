using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
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
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Fetching details for replication flow '{name}'...");

        try
        {
            // In a real implementation, this would query Connect API
            var details = new
            {
                Name = name,
                Status = "RUNNING",
                SourceCluster = new { Alias = "dc1", BootstrapServers = "dc1-kafka:9092" },
                TargetCluster = new { Alias = "dc2", BootstrapServers = "dc2-kafka:9092" },
                Topics = new[] { "orders", "payments", "users" },
                TopicsPattern = ".*",
                Tasks = 4,
                Config = new
                {
                    SyncGroupOffsets = true,
                    EmitHeartbeats = true,
                    EmitCheckpoints = true,
                    ReplicationPolicy = "DefaultReplicationPolicy"
                },
                Metrics = new
                {
                    RecordsReplicated = 1234567,
                    BytesReplicated = 567890123,
                    CurrentLagMs = 150,
                    LastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-5)
                }
            };

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(details, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"{details.Name}\t{details.Status}\t{details.SourceCluster.Alias}\t{details.TargetCluster.Alias}\t{details.TopicsPattern}\t{string.Join(",", details.Topics)}\t{details.Tasks}");
                Console.WriteLine($"  SyncGroupOffsets\t{details.Config.SyncGroupOffsets}");
                Console.WriteLine($"  EmitHeartbeats\t{details.Config.EmitHeartbeats}");
                Console.WriteLine($"  EmitCheckpoints\t{details.Config.EmitCheckpoints}");
                Console.WriteLine($"  ReplicationPolicy\t{details.Config.ReplicationPolicy}");
            }
            else
            {
                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                AnsiConsole.MarkupLine($"[bold]Replication Flow: {name}[/]\n");

                var table = new Table();
                table.AddColumn("Property");
                table.AddColumn("Value");
                table.Border = TableBorder.Rounded;

                table.AddRow("Status", $"[green]{details.Status}[/]");
                table.AddRow("Source Cluster", $"{details.SourceCluster.Alias} ({details.SourceCluster.BootstrapServers})");
                table.AddRow("Target Cluster", $"{details.TargetCluster.Alias} ({details.TargetCluster.BootstrapServers})");
                table.AddRow("Topics Pattern", details.TopicsPattern);
                table.AddRow("Active Topics", string.Join(", ", details.Topics));
                table.AddRow("Tasks", details.Tasks.ToString());

                AnsiConsole.Write(table);

                AnsiConsole.MarkupLine("\n[bold]Configuration[/]");
                var configTable = new Table();
                configTable.AddColumn("Setting");
                configTable.AddColumn("Value");
                configTable.Border = TableBorder.Simple;

                configTable.AddRow("Sync Group Offsets", details.Config.SyncGroupOffsets ? "yes" : "no");
                configTable.AddRow("Emit Heartbeats", details.Config.EmitHeartbeats ? "yes" : "no");
                configTable.AddRow("Emit Checkpoints", details.Config.EmitCheckpoints ? "yes" : "no");
                configTable.AddRow("Replication Policy", details.Config.ReplicationPolicy);

                AnsiConsole.Write(configTable);

                AnsiConsole.MarkupLine("\n[bold]Metrics[/]");
                var metricsTable = new Table();
                metricsTable.AddColumn("Metric");
                metricsTable.AddColumn("Value");
                metricsTable.Border = TableBorder.Simple;

                metricsTable.AddRow("Records Replicated", details.Metrics.RecordsReplicated.ToString("N0"));
                metricsTable.AddRow("Bytes Replicated", FormatBytes(details.Metrics.BytesReplicated));
                metricsTable.AddRow("Current Lag", $"{details.Metrics.CurrentLagMs}ms");
                metricsTable.AddRow("Last Heartbeat", details.Metrics.LastHeartbeat.ToString("o"));

                AnsiConsole.Write(metricsTable);
            }

            await Task.CompletedTask;
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB", "TB"];
        int index = 0;
        double size = bytes;

        while (size >= 1024 && index < suffixes.Length - 1)
        {
            size /= 1024;
            index++;
        }

        return $"{size:F2} {suffixes[index]}";
    }
}
