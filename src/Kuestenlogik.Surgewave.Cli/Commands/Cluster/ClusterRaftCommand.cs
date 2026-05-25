using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Cluster Raft command (surgewave cluster raft)
/// Shows detailed Raft consensus state for the connected broker.
/// </summary>
public sealed class ClusterRaftCommand : CommandBase
{
    public ClusterRaftCommand() : base("raft", "Show Raft consensus state")
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

            var clusterInfo = await client.Cluster.GetClusterInfoAsync(ct);
            var brokers = await client.Cluster.ListBrokersAsync(ct);

            if (format == OutputFormat.Json)
            {
                var raftState = new
                {
                    Enabled = clusterInfo.UseRaftConsensus,
                    ThisBroker = new
                    {
                        Id = clusterInfo.BrokerId,
                        IsLeader = clusterInfo.IsRaftLeader,
                        IsController = clusterInfo.IsController
                    },
                    Term = clusterInfo.RaftTerm,
                    Controller = new
                    {
                        Id = clusterInfo.ControllerId,
                        Epoch = clusterInfo.ControllerEpoch
                    },
                    ClusterSize = brokers.Count,
                    Brokers = brokers.Select(b => new
                    {
                        b.BrokerId,
                        b.Host,
                        b.Port,
                        b.IsController,
                        b.IsAlive
                    })
                };
                Console.WriteLine(JsonSerializer.Serialize(raftState, ClusterJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Raft Consensus State[/]").LeftJustified());
                AnsiConsole.WriteLine();

                if (!clusterInfo.UseRaftConsensus)
                {
                    AnsiConsole.MarkupLine("[yellow]Raft consensus is not enabled on this broker.[/]");
                    AnsiConsole.MarkupLine("[dim]Use --raft-consensus=true to enable Raft mode.[/]");
                    return 0;
                }

                // Raft state grid
                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Raft Enabled:[/]", "[green]Yes[/]");
                grid.AddRow("[bold]Current Term:[/]", clusterInfo.RaftTerm.ToString());
                grid.AddRow("[bold]This Broker ID:[/]", clusterInfo.BrokerId.ToString());

                var roleDisplay = clusterInfo.IsRaftLeader
                    ? "[green bold]LEADER[/]"
                    : "[dim]Follower[/]";
                grid.AddRow("[bold]Role:[/]", roleDisplay);

                grid.AddRow("[bold]Controller ID:[/]", clusterInfo.ControllerId.ToString());
                grid.AddRow("[bold]Controller Epoch:[/]", clusterInfo.ControllerEpoch.ToString());

                AnsiConsole.Write(grid);
                AnsiConsole.WriteLine();

                // Quorum members table
                AnsiConsole.Write(new Rule("[bold]Quorum Members[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Broker ID");
                table.AddColumn("Host");
                table.AddColumn("Port");
                table.AddColumn("Role");
                table.AddColumn("Status");

                var aliveBrokers = brokers.Count(b => b.IsAlive);

                foreach (var broker in brokers.OrderBy(b => b.BrokerId))
                {
                    var role = broker.IsController ? "[green bold]Controller[/]" : "[dim]Voter[/]";
                    var status = broker.IsAlive ? "[green]Online[/]" : "[red]Offline[/]";

                    table.AddRow(
                        broker.BrokerId.ToString(),
                        broker.Host,
                        broker.Port.ToString(),
                        role,
                        status);
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();

                // Quorum health
                var quorumSize = (brokers.Count / 2) + 1;
                var hasQuorum = aliveBrokers >= quorumSize;
                var quorumStatus = hasQuorum
                    ? $"[green]Healthy ({aliveBrokers}/{brokers.Count} online, quorum={quorumSize})[/]"
                    : $"[red]Lost ({aliveBrokers}/{brokers.Count} online, need {quorumSize})[/]";

                AnsiConsole.MarkupLine($"[bold]Quorum Status:[/] {quorumStatus}");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get Raft state: {ex.Message}");
            return 1;
        }
    }
}
