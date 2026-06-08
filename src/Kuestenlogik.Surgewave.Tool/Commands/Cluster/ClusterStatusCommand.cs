using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Cluster status command (surgewave cluster status)
/// </summary>
public class ClusterStatusCommand : CommandBase
{
    private readonly Option<bool> _waitOpt = new("--wait", "-w")
    {
        Description = "Block until the broker is reachable. Useful for CI/CD startup checks. Retries every 2 seconds until --wait-timeout.",
    };

    private readonly Option<int> _waitTimeoutOpt = new("--wait-timeout")
    {
        Description = "Maximum seconds to wait when --wait is set (default: 60)",
        DefaultValueFactory = _ => 60,
    };

    public ClusterStatusCommand() : base("status", "Show cluster status")
    {
        Options.Add(_waitOpt);
        Options.Add(_waitTimeoutOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var wait = parseResult.GetValue(_waitOpt);
        var waitTimeout = parseResult.GetValue(_waitTimeoutOpt);

        if (wait)
        {
            return await WaitForBrokerAsync(host, port, waitTimeout, format, ct);
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var clusterInfo = await client.Cluster.GetClusterInfoAsync(ct);
            var topics = await client.Topics.ListAsync(ct);

            var avgPartitionsPerTopic = topics.Count > 0 ? (double)clusterInfo.TotalPartitions / topics.Count : 0;

            if (format == OutputFormat.Json)
            {
                var status = new
                {
                    Broker = new
                    {
                        Id = clusterInfo.BrokerId,
                        Host = clusterInfo.Host,
                        Port = clusterInfo.Port,
                        IsController = clusterInfo.IsController
                    },
                    Controller = new
                    {
                        Id = clusterInfo.ControllerId,
                        Epoch = clusterInfo.ControllerEpoch
                    },
                    Raft = new
                    {
                        Enabled = clusterInfo.UseRaftConsensus,
                        IsLeader = clusterInfo.IsRaftLeader,
                        Term = clusterInfo.RaftTerm
                    },
                    Topics = clusterInfo.TopicCount,
                    TotalPartitions = clusterInfo.TotalPartitions,
                    AveragePartitionsPerTopic = avgPartitionsPerTopic
                };
                Console.WriteLine(JsonSerializer.Serialize(status, ClusterJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"connected\t{clusterInfo.Host}:{clusterInfo.Port}");
                Console.WriteLine($"broker\t{clusterInfo.BrokerId}\t{(clusterInfo.IsController ? "controller" : "follower")}");
                Console.WriteLine($"controller\t{clusterInfo.ControllerId}\tepoch={clusterInfo.ControllerEpoch}");
                Console.WriteLine($"raft\t{(clusterInfo.UseRaftConsensus ? "enabled" : "disabled")}\tleader={(clusterInfo.IsRaftLeader ? "yes" : "no")}\tterm={clusterInfo.RaftTerm}");
                Console.WriteLine($"topics\t{clusterInfo.TopicCount}\tpartitions={clusterInfo.TotalPartitions}");
                foreach (var topic in topics.OrderBy(t => t.Name))
                    Console.WriteLine($"topic\t{topic.Name}\t{topic.PartitionCount}");
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Cluster Status[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Connected to:[/]", $"{clusterInfo.Host}:{clusterInfo.Port}");
                grid.AddRow("[bold]Broker ID:[/]", clusterInfo.BrokerId.ToString());
                grid.AddRow("[bold]Is Controller:[/]", clusterInfo.IsController ? "[green]Yes[/]" : "No");
                grid.AddRow("[bold]Controller ID:[/]", clusterInfo.ControllerId.ToString());
                grid.AddRow("[bold]Controller Epoch:[/]", clusterInfo.ControllerEpoch.ToString());

                if (clusterInfo.UseRaftConsensus)
                {
                    grid.AddRow("[bold]Raft Consensus:[/]", "[green]Enabled[/]");
                    grid.AddRow("[bold]Raft Leader:[/]", clusterInfo.IsRaftLeader ? "[green]Yes[/]" : "No");
                    grid.AddRow("[bold]Raft Term:[/]", clusterInfo.RaftTerm.ToString());
                }
                else
                {
                    grid.AddRow("[bold]Raft Consensus:[/]", "[dim]Disabled[/]");
                }

                grid.AddRow("", "");
                grid.AddRow("[bold]Topics:[/]", clusterInfo.TopicCount.ToString());
                grid.AddRow("[bold]Total Partitions:[/]", clusterInfo.TotalPartitions.ToString());
                grid.AddRow("[bold]Avg Partitions/Topic:[/]", $"{avgPartitionsPerTopic:F1}");

                AnsiConsole.Write(grid);
                AnsiConsole.WriteLine();

                // Topic summary table
                if (topics.Count > 0)
                {
                    AnsiConsole.Write(new Rule("[bold]Topics[/]").LeftJustified());
                    AnsiConsole.WriteLine();

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partitions");

                    foreach (var topic in topics.OrderBy(t => t.Name))
                    {
                        table.AddRow(topic.Name, topic.PartitionCount.ToString());
                    }

                    AnsiConsole.Write(table);
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get cluster status: {ex.Message}");
            return 1;
        }
    }

    private async Task<int> WaitForBrokerAsync(string host, int port, int timeoutSeconds, OutputFormat format, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        var attempt = 0;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                await using var client = new SurgewaveNativeClient(host, port);
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(5));
                await client.ConnectAsync(cts.Token);
                var clusterInfo = await client.Cluster.GetClusterInfoAsync(cts.Token);

                // Success — print status and exit 0
                if (format == OutputFormat.Json)
                {
                    System.Console.Out.WriteLine(JsonSerializer.Serialize(new
                    {
                        status = "ready",
                        attempts = attempt,
                        broker = $"{host}:{port}",
                        brokerId = clusterInfo.BrokerId,
                        topics = clusterInfo.TopicCount,
                    }, ClusterJsonOptions.Indented));
                }
                else
                {
                    AnsiConsole.MarkupLine($"[green]Broker ready[/] at {host}:{port} (attempt {attempt})");
                }
                return 0;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Per-attempt timeout — retry
            }
            catch
            {
                // Connection failed — retry
            }

            if (format != OutputFormat.Json)
            {
                AnsiConsole.MarkupLine($"[dim]Waiting for broker at {host}:{port}... (attempt {attempt})[/]");
            }
            await Task.Delay(2000, ct);
        }

        WriteError($"Broker at {host}:{port} not reachable after {timeoutSeconds}s ({attempt} attempts)");
        return 1;
    }
}
