using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Cluster balance command (surgewave cluster balance ...)
/// </summary>
public class ClusterBalanceCommand : CommandBase
{
    private readonly Option<bool> _statusOpt = new("--status") { Description = "Show balance status" };
    private readonly Option<bool> _dryRunOpt = new("--dry-run") { Description = "Preview rebalance plan without executing" };
    private readonly Option<bool> _executeOpt = new("--execute") { Description = "Execute rebalance" };
    private readonly Option<string?> _topicsOpt = new("--topics", "-t") { Description = "Comma-separated list of topics to rebalance (all if not specified)" };

    public ClusterBalanceCommand() : base("balance", "Check and manage cluster balance")
    {
        Options.Add(_statusOpt);
        Options.Add(_dryRunOpt);
        Options.Add(_executeOpt);
        Options.Add(_topicsOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var status = parseResult.GetValue(_statusOpt);
        var dryRun = parseResult.GetValue(_dryRunOpt);
        var execute = parseResult.GetValue(_executeOpt);
        var topicsStr = parseResult.GetValue(_topicsOpt);

        // Default to status if nothing specified
        if (!status && !dryRun && !execute)
        {
            status = true;
        }

        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            var allTopics = await client.Topics.ListAsync(ct);

            // Filter topics if specified
            var targetTopics = allTopics;
            if (!string.IsNullOrEmpty(topicsStr))
            {
                var topicNames = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(t => t.Trim())
                    .ToHashSet();
                targetTopics = allTopics.Where(t => topicNames.Contains(t.Name)).ToList();
            }

            var totalPartitions = targetTopics.Sum(t => t.PartitionCount);

            if (status)
            {
                ShowBalanceStatus(format, host, port, targetTopics, totalPartitions);
            }
            else if (dryRun)
            {
                ShowDryRun(format, targetTopics, totalPartitions);
            }
            else if (execute)
            {
                ShowExecuteRebalance(format, targetTopics);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to check balance: {ex.Message}");
            return 1;
        }
    }

    private static void ShowBalanceStatus(
        OutputFormat format,
        string host,
        int port,
        List<TopicInfo> topics,
        int totalPartitions)
    {
        // Calculate partition distribution across topics
        var maxPartitions = topics.Max(t => t.PartitionCount);
        var minPartitions = topics.Min(t => t.PartitionCount);
        var avgPartitions = topics.Count > 0 ? (double)totalPartitions / topics.Count : 0;

        // Simple balance metric based on partition distribution
        double variance = topics.Sum(t => Math.Pow(t.PartitionCount - avgPartitions, 2)) / Math.Max(1, topics.Count);
        double stdDev = Math.Sqrt(variance);
        double balanceScore = avgPartitions > 0 ? Math.Max(0, 1 - (stdDev / avgPartitions)) : 1;

        string balanceState = balanceScore > 0.9 ? "Balanced" :
            balanceScore > 0.7 ? "Minor Imbalance" :
            balanceScore > 0.5 ? "Imbalanced" : "Critical";

        if (format == OutputFormat.Json)
        {
            var status = new
            {
                Broker = new { Host = host, Port = port },
                Balance = new
                {
                    State = balanceState,
                    Score = Math.Round(balanceScore, 2),
                    Topics = topics.Count,
                    TotalPartitions = totalPartitions,
                    MinPartitionsPerTopic = minPartitions,
                    MaxPartitionsPerTopic = maxPartitions,
                    AvgPartitionsPerTopic = Math.Round(avgPartitions, 1)
                }
            };
            Console.WriteLine(JsonSerializer.Serialize(status, ClusterJsonOptions.Indented));
        }
        else
        {
            var stateColor = balanceState switch
            {
                "Balanced" => "green",
                "Minor Imbalance" => "yellow",
                "Imbalanced" => "orange1",
                _ => "red"
            };

            AnsiConsole.Write(new Rule("[bold blue]Cluster Balance Status[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var grid = new Grid();
            grid.AddColumn();
            grid.AddColumn();

            grid.AddRow("[bold]State:[/]", $"[{stateColor}]{balanceState}[/]");
            grid.AddRow("[bold]Balance Score:[/]", $"{balanceScore:P0}");
            grid.AddRow("[bold]Topics:[/]", topics.Count.ToString());
            grid.AddRow("[bold]Total Partitions:[/]", totalPartitions.ToString());
            grid.AddRow("[bold]Min/Max/Avg Partitions:[/]", $"{minPartitions} / {maxPartitions} / {avgPartitions:F1}");

            AnsiConsole.Write(grid);
            AnsiConsole.WriteLine();

            // Show topic distribution
            AnsiConsole.MarkupLine("[bold]Partition Distribution:[/]");
            AnsiConsole.WriteLine();

            foreach (var topic in topics.OrderByDescending(t => t.PartitionCount).Take(10))
            {
                var bar = new string('#', Math.Max(1, topic.PartitionCount * 50 / Math.Max(1, maxPartitions)));
                var color = topic.PartitionCount > avgPartitions * 1.5 ? "yellow" :
                    topic.PartitionCount < avgPartitions * 0.5 ? "red" : "green";
                AnsiConsole.MarkupLine($"  [{color}]{topic.Name,-30}[/] [{color}]{bar}[/] {topic.PartitionCount}");
            }

            if (topics.Count > 10)
            {
                AnsiConsole.MarkupLine($"  [dim]... and {topics.Count - 10} more topics[/]");
            }

            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[dim]Note: Full cluster balance requires admin API access to view broker distribution.[/]");
        }
    }

    private static void ShowDryRun(
        OutputFormat format,
        List<TopicInfo> topics,
        int totalPartitions)
    {
        if (format == OutputFormat.Json)
        {
            var plan = new
            {
                DryRun = true,
                Topics = topics.Count,
                TotalPartitions = totalPartitions,
                Message = "Rebalance dry-run requires admin API access to calculate broker assignments."
            };
            Console.WriteLine(JsonSerializer.Serialize(plan, ClusterJsonOptions.Indented));
        }
        else
        {
            AnsiConsole.Write(new Rule("[bold blue]Rebalance Dry Run[/]").LeftJustified());
            AnsiConsole.WriteLine();

            AnsiConsole.MarkupLine($"[bold]Topics:[/] {topics.Count}");
            AnsiConsole.MarkupLine($"[bold]Total Partitions:[/] {totalPartitions}");
            AnsiConsole.WriteLine();

            var table = new Table();
            table.AddColumn("Topic");
            table.AddColumn("Partitions");
            table.AddColumn("Action");

            foreach (var topic in topics.OrderBy(t => t.Name))
            {
                table.AddRow(topic.Name, topic.PartitionCount.ToString(), "[dim]Check balance[/]");
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[yellow]Note: Full rebalance planning requires admin API access.[/]");
            AnsiConsole.MarkupLine("[dim]Use 'surgewave partitions reassign --generate' for partition reassignment planning.[/]");
        }
    }

    private static void ShowExecuteRebalance(
        OutputFormat format,
        List<TopicInfo> topics)
    {
        if (format == OutputFormat.Json)
        {
            var result = new
            {
                Executed = false,
                Message = "Rebalance execution requires admin API access."
            };
            Console.WriteLine(JsonSerializer.Serialize(result, ClusterJsonOptions.Indented));
        }
        else
        {
            AnsiConsole.MarkupLine("[yellow]Rebalance execution requires admin API access.[/]");
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("To rebalance partitions manually:");
            AnsiConsole.MarkupLine("  1. Generate a reassignment plan:");
            AnsiConsole.MarkupLine("     [cyan]surgewave partitions reassign --generate --topics <topics> --brokers <ids> --file plan.json[/]");
            AnsiConsole.MarkupLine("  2. Execute the plan:");
            AnsiConsole.MarkupLine("     [cyan]surgewave partitions reassign --execute --file plan.json[/]");
            AnsiConsole.MarkupLine("  3. Verify progress:");
            AnsiConsole.MarkupLine("     [cyan]surgewave partitions reassign --verify --file plan.json[/]");
        }
    }
}
