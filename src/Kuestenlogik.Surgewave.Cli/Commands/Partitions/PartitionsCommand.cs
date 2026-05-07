using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Partitions;

internal static class PartitionsJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for managing partitions (surgewave partitions ...)
/// </summary>
public class PartitionsCommand : CommandBase
{
    public PartitionsCommand() : base("partitions", "Manage partition assignments")
    {
        Subcommands.Add(new DescribePartitionsCommand());
        Subcommands.Add(new ReassignPartitionsCommand());
        Subcommands.Add(new ElectLeaderCommand());
        Subcommands.Add(new BalancePartitionsCommand());
        Subcommands.Add(new DecommissionCommand());
        Subcommands.Add(new AssignmentCommand());
        Subcommands.Add(new ReassignStatusCommand());
    }
}

/// <summary>
/// Describe partitions for a topic (surgewave partitions describe)
/// </summary>
public class DescribePartitionsCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Topic name" };

    public DescribePartitionsCommand() : base("describe", "Describe partition assignments for a topic")
    {
        Arguments.Add(_topicArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            var topics = await client.Topics.ListAsync(ct);
            var topicInfo = topics.FirstOrDefault(t => t.Name == topic);

            if (topicInfo == null)
            {
                WriteError($"Topic '{topic}' not found");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                var partitions = new List<object>();
                for (int i = 0; i < topicInfo.PartitionCount; i++)
                {
                    var offset = await client.Messaging.GetLatestOffsetAsync(topic, i, ct);
                    partitions.Add(new { Partition = i, HighWatermark = offset });
                }
                var result = new { Topic = topic, topicInfo.PartitionCount, Partitions = partitions };
                Console.WriteLine(JsonSerializer.Serialize(result, PartitionsJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Topic:[/] {topic}");
                AnsiConsole.MarkupLine($"[bold]Partitions:[/] {topicInfo.PartitionCount}");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Partition");
                table.AddColumn("High Watermark");

                for (int i = 0; i < topicInfo.PartitionCount; i++)
                {
                    var offset = await client.Messaging.GetLatestOffsetAsync(topic, i, ct);
                    table.AddRow(i.ToString(), offset.ToString());
                }

                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe partitions: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>
/// Reassign partitions (surgewave partitions reassign)
/// </summary>
public class ReassignPartitionsCommand : CommandBase
{
    private readonly Option<string?> _fileOpt = new("--file", "-f") { Description = "Reassignment plan JSON file" };
    private readonly Option<string?> _topicsOpt = new("--topics", "-t") { Description = "Comma-separated list of topics to reassign" };
    private readonly Option<string?> _brokersOpt = new("--brokers", "-b") { Description = "Comma-separated list of broker IDs" };
    private readonly Option<bool> _generateOpt = new("--generate") { Description = "Generate a reassignment plan" };
    private readonly Option<bool> _executeOpt = new("--execute") { Description = "Execute the reassignment plan" };
    private readonly Option<bool> _verifyOpt = new("--verify") { Description = "Verify reassignment progress" };

    public ReassignPartitionsCommand() : base("reassign", "Reassign partition replicas")
    {
        Options.Add(_fileOpt);
        Options.Add(_topicsOpt);
        Options.Add(_brokersOpt);
        Options.Add(_generateOpt);
        Options.Add(_executeOpt);
        Options.Add(_verifyOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var file = parseResult.GetValue(_fileOpt);
        var topics = parseResult.GetValue(_topicsOpt);
        var brokers = parseResult.GetValue(_brokersOpt);
        var generate = parseResult.GetValue(_generateOpt);
        var execute = parseResult.GetValue(_executeOpt);
        var verify = parseResult.GetValue(_verifyOpt);

        if (generate)
        {
            return await GeneratePlanAsync(parseResult, topics, brokers, file, ct);
        }
        else if (execute)
        {
            return await ExecutePlanAsync(parseResult, file, ct);
        }
        else if (verify)
        {
            return await VerifyPlanAsync(parseResult, file, ct);
        }
        else
        {
            WriteError("Specify one of: --generate, --execute, or --verify");
            return 1;
        }
    }

    private async Task<int> GeneratePlanAsync(ParseResult parseResult, string? topicsStr, string? brokersStr, string? outputFile, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(topicsStr))
        {
            WriteError("--topics is required for --generate");
            return 1;
        }

        if (string.IsNullOrEmpty(brokersStr))
        {
            WriteError("--brokers is required for --generate");
            return 1;
        }

        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topicList = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(t => t.Trim()).ToList();
        var brokerList = brokersStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(b => int.Parse(b.Trim()))
            .ToList();

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var plan = new ReassignmentPlan { Version = 1, Partitions = [] };
            var allTopics = await client.Topics.ListAsync(ct);

            foreach (var topic in topicList)
            {
                var topicInfo = allTopics.FirstOrDefault(t => t.Name == topic);
                if (topicInfo == null) continue;

                for (int partition = 0; partition < topicInfo.PartitionCount; partition++)
                {
                    var newReplicas = new List<int>();
                    var startIndex = partition % brokerList.Count;
                    var replicationFactor = 1; // Default, would need metadata API for actual RF

                    for (int i = 0; i < Math.Min(replicationFactor, brokerList.Count); i++)
                    {
                        var idx = (startIndex + i) % brokerList.Count;
                        newReplicas.Add(brokerList[idx]);
                    }

                    plan.Partitions.Add(new PartitionReassignment
                    {
                        Topic = topic,
                        Partition = partition,
                        Replicas = newReplicas
                    });
                }
            }

            var json = JsonSerializer.Serialize(plan, PartitionsJsonOptions.Indented);

            if (!string.IsNullOrEmpty(outputFile))
            {
                await File.WriteAllTextAsync(outputFile, json, ct);
                WriteSuccess($"Reassignment plan written to {outputFile}");
                AnsiConsole.MarkupLine($"[dim]Partitions to reassign: {plan.Partitions.Count}[/]");
            }
            else
            {
                Console.WriteLine(json);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to generate plan: {ex.Message}");
            return 1;
        }
    }

    private async Task<int> ExecutePlanAsync(ParseResult parseResult, string? file, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(file))
        {
            WriteError("--file is required for --execute");
            return 1;
        }

        if (!File.Exists(file))
        {
            WriteError($"File not found: {file}");
            return 1;
        }

        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            var json = await File.ReadAllTextAsync(file, ct);
            var plan = JsonSerializer.Deserialize<ReassignmentPlan>(json);

            if (plan == null || plan.Partitions.Count == 0)
            {
                WriteError("No partitions to reassign in plan");
                return 1;
            }

            // Convert to client request format
            var reassignments = plan.Partitions
                .Select(p => new PartitionReassignmentRequest(p.Topic, p.Partition, p.Replicas))
                .ToList();

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var result = await client.Cluster.AlterPartitionReassignmentsAsync(reassignments, ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    result.Success,
                    result.PartitionCount,
                    ErrorCode = result.ErrorCode.ToString()
                };
                Console.WriteLine(JsonSerializer.Serialize(output, PartitionsJsonOptions.Indented));
            }
            else
            {
                if (result.Success)
                {
                    WriteSuccess($"Reassignment started for {result.PartitionCount} partition(s)");

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("New Replicas");

                    foreach (var p in plan.Partitions.OrderBy(p => p.Topic).ThenBy(p => p.Partition))
                    {
                        table.AddRow(p.Topic, p.Partition.ToString(), string.Join(", ", p.Replicas));
                    }

                    AnsiConsole.Write(table);
                    AnsiConsole.WriteLine();
                    AnsiConsole.MarkupLine("[dim]Use --verify to check progress.[/]");
                }
                else
                {
                    WriteError($"Reassignment failed: {result.ErrorCode}");
                    return 1;
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to execute plan: {ex.Message}");
            return 1;
        }
    }

    private async Task<int> VerifyPlanAsync(ParseResult parseResult, string? file, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var reassignments = await client.Cluster.ListPartitionReassignmentsAsync(ct);

            // If file provided, filter to only partitions in the plan
            if (!string.IsNullOrEmpty(file) && File.Exists(file))
            {
                var json = await File.ReadAllTextAsync(file, ct);
                var plan = JsonSerializer.Deserialize<ReassignmentPlan>(json);

                if (plan != null)
                {
                    var planPartitions = plan.Partitions
                        .Select(p => (p.Topic, p.Partition))
                        .ToHashSet();

                    reassignments = reassignments
                        .Where(r => planPartitions.Contains((r.Topic, r.Partition)))
                        .ToList();
                }
            }

            if (format == OutputFormat.Json)
            {
                var output = reassignments.Select(r => new
                {
                    r.Topic,
                    r.Partition,
                    Status = r.Status.ToString(),
                    r.ProgressPercent,
                    r.OriginalReplicas,
                    r.TargetReplicas
                });
                Console.WriteLine(JsonSerializer.Serialize(output, PartitionsJsonOptions.Indented));
            }
            else
            {
                if (reassignments.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No active reassignments found.[/]");
                    return 0;
                }

                AnsiConsole.MarkupLine("[bold]Active Partition Reassignments[/]");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Topic");
                table.AddColumn("Partition");
                table.AddColumn("Status");
                table.AddColumn("Progress");
                table.AddColumn("Original");
                table.AddColumn("Target");

                foreach (var r in reassignments.OrderBy(r => r.Topic).ThenBy(r => r.Partition))
                {
                    var statusColor = r.Status switch
                    {
                        ReassignmentStatusCode.Completed => "green",
                        ReassignmentStatusCode.Failed or ReassignmentStatusCode.Cancelled => "red",
                        ReassignmentStatusCode.Syncing => "yellow",
                        _ => "blue"
                    };

                    table.AddRow(
                        r.Topic,
                        r.Partition.ToString(),
                        $"[{statusColor}]{r.Status}[/]",
                        $"{r.ProgressPercent}%",
                        string.Join(",", r.OriginalReplicas),
                        string.Join(",", r.TargetReplicas));
                }

                AnsiConsole.Write(table);

                var completed = reassignments.Count(r => r.Status == ReassignmentStatusCode.Completed);
                var inProgress = reassignments.Count(r => r.Status is ReassignmentStatusCode.Pending
                    or ReassignmentStatusCode.Adding or ReassignmentStatusCode.Syncing
                    or ReassignmentStatusCode.Completing);
                var failed = reassignments.Count(r => r.Status == ReassignmentStatusCode.Failed);

                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Completed: {completed}, In Progress: {inProgress}, Failed: {failed}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to verify reassignments: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>
/// Elect preferred leaders for partitions (surgewave partitions elect-leader)
/// </summary>
public class ElectLeaderCommand : CommandBase
{
    private readonly Option<string?> _topicOpt = new("--topic", "-t") { Description = "Topic name (all topics if omitted)" };
    private readonly Option<int[]?> _partitionsOpt = new("--partitions", "-p") { Description = "Partition IDs (comma-separated)", AllowMultipleArgumentsPerToken = true };
    private readonly Option<string> _electionTypeOpt = new("--election-type", "-e") { Description = "Election type: preferred or unclean", DefaultValueFactory = _ => "preferred" };
    private readonly Option<bool> _allOpt = new("--all", "-a") { Description = "Elect leaders for all partitions" };

    public ElectLeaderCommand() : base("elect-leader", "Trigger leader election for partitions")
    {
        Options.Add(_topicOpt);
        Options.Add(_partitionsOpt);
        Options.Add(_electionTypeOpt);
        Options.Add(_allOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var topic = parseResult.GetValue(_topicOpt);
        var partitions = parseResult.GetValue(_partitionsOpt);
        var electionTypeStr = parseResult.GetValue(_electionTypeOpt);
        var all = parseResult.GetValue(_allOpt);

        var electionType = electionTypeStr?.ToLowerInvariant() == "unclean"
            ? ElectionType.Unclean
            : ElectionType.Preferred;

        WriteVerbose(parseResult, $"Triggering {electionType} leader election...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var partitionList = new List<(string Topic, int Partition)>();

            if (all)
            {
                // Get all topics and their partitions
                var topics = await client.Topics.ListAsync(ct);
                foreach (var t in topics)
                {
                    for (int i = 0; i < t.PartitionCount; i++)
                    {
                        partitionList.Add((t.Name, i));
                    }
                }
            }
            else
            {
                if (string.IsNullOrEmpty(topic))
                {
                    WriteError("Either specify --topic or use --all for all partitions");
                    return 1;
                }

                if (partitions != null && partitions.Length > 0)
                {
                    foreach (var p in partitions)
                    {
                        partitionList.Add((topic, p));
                    }
                }
                else
                {
                    // Get all partitions for the topic
                    var topics = await client.Topics.ListAsync(ct);
                    var topicInfo = topics.FirstOrDefault(t => t.Name == topic);

                    if (topicInfo == null)
                    {
                        WriteError($"Topic '{topic}' not found");
                        return 1;
                    }

                    for (int i = 0; i < topicInfo.PartitionCount; i++)
                    {
                        partitionList.Add((topic, i));
                    }
                }
            }

            if (partitionList.Count == 0)
            {
                AnsiConsole.MarkupLine("[dim]No partitions matched the criteria[/]");
                return 0;
            }

            var results = await client.Admin.ElectLeaderAsync(partitionList, electionType, ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    ElectionType = electionType.ToString(),
                    Results = results.Select(r => new
                    {
                        r.Topic,
                        r.Partition,
                        Success = r.ErrorCode == SurgewaveErrorCode.None,
                        Error = r.ErrorCode == SurgewaveErrorCode.None ? null : r.ErrorMessage
                    }).ToList()
                };
                Console.WriteLine(JsonSerializer.Serialize(output, PartitionsJsonOptions.Indented));
            }
            else
            {
                var succeeded = results.Count(r => r.ErrorCode == SurgewaveErrorCode.None);
                var failed = results.Count(r => r.ErrorCode != SurgewaveErrorCode.None);

                if (succeeded > 0)
                {
                    WriteSuccess($"{electionType} leader election triggered for {succeeded} partition(s)");
                }

                if (failed > 0)
                {
                    AnsiConsole.WriteLine();
                    AnsiConsole.MarkupLine($"[yellow]Failed for {failed} partition(s):[/]");

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("Error");

                    foreach (var r in results.Where(r => r.ErrorCode != SurgewaveErrorCode.None))
                    {
                        table.AddRow(
                            r.Topic,
                            r.Partition.ToString(),
                            $"[red]{r.ErrorMessage ?? r.ErrorCode.ToString()}[/]"
                        );
                    }

                    AnsiConsole.Write(table);
                }

                if (succeeded == 0 && failed == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No partitions matched the criteria[/]");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to elect leaders: {ex.Message}");
            return 1;
        }
    }
}
