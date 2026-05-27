using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Cluster nodes command (surgewave cluster nodes)
/// </summary>
public class ClusterNodesCommand : CommandBase
{
    public ClusterNodesCommand() : base("nodes", "List all nodes/brokers in the cluster")
    {
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
            var brokers = await client.Cluster.ListBrokersAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    Brokers = brokers.Select(b => new
                    {
                        b.BrokerId,
                        b.Host,
                        b.Port,
                        b.ReplicationPort,
                        b.IsController,
                        b.IsAlive,
                        b.Rack
                    })
                };
                Console.WriteLine(JsonSerializer.Serialize(output, ClusterJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Cluster Nodes[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Broker ID");
                table.AddColumn("Host");
                table.AddColumn("Port");
                table.AddColumn("Replication Port");
                table.AddColumn("Controller");
                table.AddColumn("Status");
                table.AddColumn("Rack");

                foreach (var broker in brokers.OrderBy(b => b.BrokerId))
                {
                    var controllerMark = broker.IsController ? "[green]●[/]" : "";
                    var statusMark = broker.IsAlive ? "[green]Online[/]" : "[red]Offline[/]";
                    table.AddRow(
                        broker.BrokerId.ToString(),
                        broker.Host,
                        broker.Port.ToString(),
                        broker.ReplicationPort.ToString(),
                        controllerMark,
                        statusMark,
                        broker.Rack ?? "-");
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Total nodes: {brokers.Count}[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list cluster nodes: {ex.Message}");
            return 1;
        }
    }
}
