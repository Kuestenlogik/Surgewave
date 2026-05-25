using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Cluster partitions command (surgewave cluster partitions)
/// Shows partition assignments, leaders, and ISR across the cluster.
/// </summary>
public sealed class ClusterPartitionsCommand : CommandBase
{
    private readonly Option<string?> _topicOption = new("--topic", "-t") { Description = "Filter by topic name (optional)" };

    public ClusterPartitionsCommand() : base("partitions", "Show partition assignments and ISR")
    {
        Options.Add(_topicOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var topicFilter = parseResult.GetValue(_topicOption);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var topics = await client.Topics.ListAsync(ct);

            // Filter topics if specified
            if (!string.IsNullOrEmpty(topicFilter))
            {
                topics = topics.Where(t =>
                    t.Name.Contains(topicFilter, StringComparison.OrdinalIgnoreCase)).ToList();
            }

            if (format == OutputFormat.Json)
            {
                var topicDetails = new List<object>();
                foreach (var topic in topics.OrderBy(t => t.Name))
                {
                    var description = await client.Topics.DescribeAsync(topic.Name, ct);
                    topicDetails.Add(new
                    {
                        description.Name,
                        description.PartitionCount,
                        description.ReplicationFactor,
                        description.IsInternal,
                        Partitions = description.Partitions.Select(p => new
                        {
                            p.PartitionId,
                            p.Leader,
                            p.LeaderEpoch,
                            p.Replicas,
                            p.Isr,
                            p.HighWatermark,
                            p.LogStartOffset,
                            IsrComplete = p.Isr.Length == p.Replicas.Length
                        })
                    });
                }

                var output = new
                {
                    TotalTopics = topics.Count,
                    TotalPartitions = topics.Sum(t => t.PartitionCount),
                    Topics = topicDetails
                };
                Console.WriteLine(JsonSerializer.Serialize(output, ClusterJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Partition Assignments[/]").LeftJustified());
                AnsiConsole.WriteLine();

                if (topics.Count == 0)
                {
                    AnsiConsole.MarkupLine("[yellow]No topics found.[/]");
                    return 0;
                }

                var totalPartitions = 0;
                var underReplicatedPartitions = 0;

                foreach (var topic in topics.OrderBy(t => t.Name))
                {
                    var description = await client.Topics.DescribeAsync(topic.Name, ct);

                    AnsiConsole.MarkupLine($"[bold]{Markup.Escape(description.Name)}[/] ({description.PartitionCount} partitions, RF={description.ReplicationFactor})");

                    var table = new Table();
                    table.Border(TableBorder.Rounded);
                    table.AddColumn("Partition");
                    table.AddColumn("Leader");
                    table.AddColumn("Replicas");
                    table.AddColumn("ISR");
                    table.AddColumn("HWM");
                    table.AddColumn("Status");

                    foreach (var partition in description.Partitions.OrderBy(p => p.PartitionId))
                    {
                        totalPartitions++;

                        var replicasStr = string.Join(",", partition.Replicas);
                        var isrStr = string.Join(",", partition.Isr);

                        var isUnderReplicated = partition.Isr.Length < partition.Replicas.Length;
                        if (isUnderReplicated) underReplicatedPartitions++;

                        var leaderDisplay = partition.Leader >= 0
                            ? partition.Leader.ToString()
                            : "[red]None[/]";

                        var status = isUnderReplicated
                            ? "[yellow]Under-replicated[/]"
                            : "[green]OK[/]";

                        table.AddRow(
                            partition.PartitionId.ToString(),
                            leaderDisplay,
                            replicasStr,
                            isrStr,
                            partition.HighWatermark.ToString(),
                            status);
                    }

                    AnsiConsole.Write(table);
                    AnsiConsole.WriteLine();
                }

                // Summary
                AnsiConsole.Write(new Rule("[bold]Summary[/]").LeftJustified());
                AnsiConsole.MarkupLine($"[bold]Total Topics:[/] {topics.Count}");
                AnsiConsole.MarkupLine($"[bold]Total Partitions:[/] {totalPartitions}");

                if (underReplicatedPartitions > 0)
                {
                    AnsiConsole.MarkupLine($"[bold]Under-replicated:[/] [yellow]{underReplicatedPartitions}[/]");
                }
                else
                {
                    AnsiConsole.MarkupLine($"[bold]Under-replicated:[/] [green]0[/]");
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get partition info: {ex.Message}");
            return 1;
        }
    }
}
