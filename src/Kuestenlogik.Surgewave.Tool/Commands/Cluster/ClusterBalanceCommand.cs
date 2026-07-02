using System.CommandLine;
using System.CommandLine.Parsing;
using System.Net.Http.Json;
using System.Text.Json;
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
    private readonly Option<string?> _topicsOpt = new("--topics", "-t") { Description = "Comma-separated list of topics (status/dry-run only; --execute always balances all topics)" };

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
            using var http = BrokerAdminHttp.Create(host);

            if (execute)
            {
                return await ExecuteRebalanceAsync(http, format, topicsStr, ct);
            }

            // --status and --dry-run both need the current assignment
            var response = await http.GetAsync("/api/partitions/assignment", ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to get partition assignment: {response.StatusCode} — {json}");
                return 1;
            }

            var assignments = ParseAssignments(json);
            var targetAssignments = FilterTopics(assignments, topicsStr);

            if (status)
            {
                return ShowBalanceStatus(format, host, port, assignments, targetAssignments);
            }

            return await ShowDryRunAsync(http, format, assignments, targetAssignments, ct);
        }
        catch (Exception ex)
        {
            WriteError($"Failed to check balance: {ex.Message}");
            return 1;
        }
    }

    private static List<PartitionAssignment> ParseAssignments(string json)
    {
        var assignments = new List<PartitionAssignment>();

        using var doc = JsonDocument.Parse(json);
        foreach (var a in doc.RootElement.EnumerateArray())
        {
            var replicas = a.GetProperty("replicas").EnumerateArray().Select(r => r.GetInt32()).ToList();
            var isr = a.GetProperty("isr").EnumerateArray().Select(r => r.GetInt32()).ToList();

            assignments.Add(new PartitionAssignment(
                a.GetProperty("topic").GetString() ?? "",
                a.GetProperty("partition").GetInt32(),
                a.GetProperty("leader").GetInt32(),
                replicas,
                isr,
                a.GetProperty("sizeBytes").GetInt64()));
        }

        return assignments;
    }

    private static List<PartitionAssignment> FilterTopics(List<PartitionAssignment> assignments, string? topicsStr)
    {
        if (string.IsNullOrEmpty(topicsStr))
        {
            return assignments;
        }

        var topicNames = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim())
            .ToHashSet();
        return assignments.Where(a => topicNames.Contains(a.Topic)).ToList();
    }

    private static int ShowBalanceStatus(
        OutputFormat format,
        string host,
        int port,
        List<PartitionAssignment> allAssignments,
        List<PartitionAssignment> assignments)
    {
        // Broker set is derived from the full cluster assignment
        var brokerIds = allAssignments.SelectMany(a => a.Replicas).Distinct().OrderBy(id => id).ToList();

        if (assignments.Count == 0 || brokerIds.Count == 0)
        {
            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(
                    new { Broker = new { Host = host, Port = port }, Message = "No partition assignments found." },
                    ClusterJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine("no-assignments");
            }
            else
            {
                AnsiConsole.MarkupLine("[dim]No partition assignments found.[/]");
            }
            return 0;
        }

        var perBroker = brokerIds.Select(id => new BrokerLoad(
            id,
            assignments.Count(a => a.Leader == id),
            assignments.Count(a => a.Replicas.Contains(id)),
            assignments.Where(a => a.Replicas.Contains(id)).Sum(a => a.SizeBytes))).ToList();

        // Balance metric based on the real replica distribution across brokers
        var totalReplicas = perBroker.Sum(b => b.Replicas);
        var avgReplicas = (double)totalReplicas / brokerIds.Count;
        var variance = perBroker.Sum(b => Math.Pow(b.Replicas - avgReplicas, 2)) / brokerIds.Count;
        var stdDev = Math.Sqrt(variance);
        var balanceScore = avgReplicas > 0 ? Math.Max(0, 1 - (stdDev / avgReplicas)) : 1;

        string balanceState = balanceScore > 0.9 ? "Balanced" :
            balanceScore > 0.7 ? "Minor Imbalance" :
            balanceScore > 0.5 ? "Imbalanced" : "Critical";

        var topicCount = assignments.Select(a => a.Topic).Distinct().Count();
        var underReplicated = assignments.Count(a => a.Isr.Count < a.Replicas.Count);

        if (format == OutputFormat.Json)
        {
            var status = new
            {
                Broker = new { Host = host, Port = port },
                Balance = new
                {
                    State = balanceState,
                    Score = Math.Round(balanceScore, 2),
                    Brokers = brokerIds.Count,
                    Topics = topicCount,
                    TotalPartitions = assignments.Count,
                    UnderReplicatedPartitions = underReplicated,
                    MinReplicasPerBroker = perBroker.Min(b => b.Replicas),
                    MaxReplicasPerBroker = perBroker.Max(b => b.Replicas),
                    AvgReplicasPerBroker = Math.Round(avgReplicas, 1)
                },
                BrokerLoad = perBroker
            };
            Console.WriteLine(JsonSerializer.Serialize(status, ClusterJsonOptions.Indented));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"balance\t{balanceState}\t{Math.Round(balanceScore, 2)}\t{brokerIds.Count}\t{assignments.Count}");
            foreach (var b in perBroker)
            {
                Console.WriteLine($"broker\t{b.BrokerId}\t{b.Leaders}\t{b.Replicas}\t{b.SizeBytes}");
            }
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
            grid.AddRow("[bold]Brokers:[/]", brokerIds.Count.ToString());
            grid.AddRow("[bold]Topics:[/]", topicCount.ToString());
            grid.AddRow("[bold]Total Partitions:[/]", assignments.Count.ToString());
            grid.AddRow("[bold]Under-Replicated:[/]", underReplicated.ToString());

            AnsiConsole.Write(grid);
            AnsiConsole.WriteLine();

            AnsiConsole.MarkupLine("[bold]Replica Distribution:[/]");
            AnsiConsole.WriteLine();

            var maxReplicas = perBroker.Max(b => b.Replicas);
            foreach (var b in perBroker)
            {
                var bar = new string('#', Math.Max(1, b.Replicas * 50 / Math.Max(1, maxReplicas)));
                var color = b.Replicas > avgReplicas * 1.5 ? "yellow" :
                    b.Replicas < avgReplicas * 0.5 ? "red" : "green";
                AnsiConsole.MarkupLine($"  [{color}]broker {b.BrokerId,-4}[/] [{color}]{bar}[/] {b.Replicas} replicas, {b.Leaders} leaders");
            }

            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[dim]Use 'surgewave cluster balance --dry-run' to preview a rebalance plan.[/]");
        }

        return 0;
    }

    private static async Task<int> ShowDryRunAsync(
        HttpClient http,
        OutputFormat format,
        List<PartitionAssignment> allAssignments,
        List<PartitionAssignment> assignments,
        CancellationToken ct)
    {
        var brokerIds = allAssignments.SelectMany(a => a.Replicas).Distinct().OrderBy(id => id).ToList();

        if (assignments.Count == 0 || brokerIds.Count == 0)
        {
            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(
                    new { DryRun = true, Message = "No partition assignments found." },
                    ClusterJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.MarkupLine("[dim]No partition assignments found.[/]");
            }
            return 0;
        }

        // Same round-robin strategy the broker's auto-balance planner uses
        var moves = new List<PartitionMove>();
        var ordered = assignments.OrderBy(a => a.Topic).ThenBy(a => a.Partition);
        foreach (var assignment in ordered)
        {
            var replicationFactor = assignment.Replicas.Count;
            var newReplicas = new List<int>();
            var startIndex = assignment.Partition % brokerIds.Count;

            for (int i = 0; i < Math.Min(replicationFactor, brokerIds.Count); i++)
            {
                newReplicas.Add(brokerIds[(startIndex + i) % brokerIds.Count]);
            }

            if (!assignment.Replicas.SequenceEqual(newReplicas))
            {
                moves.Add(new PartitionMove(assignment.Topic, assignment.Partition, assignment.Replicas, newReplicas));
            }
        }

        if (moves.Count == 0)
        {
            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(
                    new { DryRun = true, Moves = Array.Empty<PartitionMove>(), Message = "No rebalancing needed." },
                    ClusterJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine("no-moves");
            }
            else
            {
                AnsiConsole.MarkupLine("[green]Cluster is already balanced. No reassignments needed.[/]");
            }
            return 0;
        }

        // Validate the proposed plan against the broker without executing it
        var request = new
        {
            assignments = moves.Select(m => new
            {
                topic = m.Topic,
                partition = m.Partition,
                targetReplicas = m.TargetReplicas,
                currentReplicas = m.CurrentReplicas
            }).ToList(),
            description = "CLI dry-run balance plan"
        };

        var response = await http.PostAsJsonAsync("/api/partitions/reassign/validate", request, ct);
        var json = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            WriteError($"Plan validation failed: {response.StatusCode} — {json}");
            return 1;
        }

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var isValid = root.GetProperty("isValid").GetBoolean();
        var errors = root.GetProperty("errors").EnumerateArray().Select(e => e.GetString() ?? "").ToList();
        var warnings = root.GetProperty("warnings").EnumerateArray().Select(w => w.GetString() ?? "").ToList();

        if (format == OutputFormat.Json)
        {
            var plan = new
            {
                DryRun = true,
                Moves = moves,
                Validation = new { IsValid = isValid, Errors = errors, Warnings = warnings }
            };
            Console.WriteLine(JsonSerializer.Serialize(plan, ClusterJsonOptions.Indented));
        }
        else if (format == OutputFormat.Plain)
        {
            foreach (var m in moves)
            {
                Console.WriteLine($"move\t{m.Topic}\t{m.Partition}\t{string.Join(",", m.CurrentReplicas)}\t{string.Join(",", m.TargetReplicas)}");
            }
            Console.WriteLine($"validation\t{isValid}\t{errors.Count}\t{warnings.Count}");
        }
        else
        {
            AnsiConsole.Write(new Rule("[bold blue]Rebalance Dry Run[/]").LeftJustified());
            AnsiConsole.WriteLine();

            AnsiConsole.MarkupLine($"[bold]Brokers:[/] {brokerIds.Count}");
            AnsiConsole.MarkupLine($"[bold]Partitions to move:[/] {moves.Count}");
            AnsiConsole.WriteLine();

            var table = new Table();
            table.AddColumn("Topic");
            table.AddColumn("Partition");
            table.AddColumn("Current Replicas");
            table.AddColumn("Target Replicas");

            foreach (var m in moves)
            {
                table.AddRow(
                    m.Topic,
                    m.Partition.ToString(),
                    string.Join(",", m.CurrentReplicas),
                    string.Join(",", m.TargetReplicas));
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();

            if (isValid)
            {
                AnsiConsole.MarkupLine("[green]Plan validated by broker: OK[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[red]Plan validation failed:[/]");
                foreach (var error in errors)
                {
                    AnsiConsole.MarkupLine($"  [red]- {Markup.Escape(error)}[/]");
                }
            }

            foreach (var warning in warnings)
            {
                AnsiConsole.MarkupLine($"  [yellow]! {Markup.Escape(warning)}[/]");
            }

            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[dim]Run 'surgewave cluster balance --execute' to apply the broker's auto-balance plan.[/]");
        }

        return isValid ? 0 : 1;
    }

    private static async Task<int> ExecuteRebalanceAsync(
        HttpClient http,
        OutputFormat format,
        string? topicsStr,
        CancellationToken ct)
    {
        if (!string.IsNullOrEmpty(topicsStr))
        {
            WriteWarning("--topics is ignored with --execute: the broker auto-balance always considers all topics.");
        }

        var response = await http.PostAsync("/api/partitions/balance", null, ct);
        var json = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            WriteError($"Rebalance failed: {response.StatusCode} — {json}");
            return 1;
        }

        if (format == OutputFormat.Json)
        {
            Console.WriteLine(json);
            return 0;
        }

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var planId = root.GetProperty("planId").GetString();
        var status = root.GetProperty("status").GetString();
        var assignments = root.GetProperty("assignments");

        if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"plan\t{planId}\t{status}\t{assignments.GetArrayLength()}");
            return 0;
        }

        if (assignments.GetArrayLength() == 0)
        {
            AnsiConsole.MarkupLine("[green]Cluster is already balanced. No reassignments needed.[/]");
            return 0;
        }

        WriteSuccess($"Balance plan {planId} submitted ({status})");

        var table = new Table();
        table.AddColumn("Topic");
        table.AddColumn("Partition");
        table.AddColumn("Current");
        table.AddColumn("Target");

        foreach (var a in assignments.EnumerateArray())
        {
            table.AddRow(
                a.GetProperty("topic").GetString() ?? "",
                a.GetProperty("partition").GetInt32().ToString(),
                FormatReplicas(a.GetProperty("currentReplicas")),
                FormatReplicas(a.GetProperty("targetReplicas")));
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"\n[dim]Use 'surgewave partitions reassign-status --plan-id {planId}' to check progress.[/]");
        return 0;
    }

    private static string FormatReplicas(JsonElement element)
    {
        var ids = new List<string>();
        foreach (var item in element.EnumerateArray())
            ids.Add(item.GetInt32().ToString());
        return string.Join(",", ids);
    }

    private sealed record PartitionAssignment(
        string Topic,
        int Partition,
        int Leader,
        List<int> Replicas,
        List<int> Isr,
        long SizeBytes);

    private sealed record BrokerLoad(int BrokerId, int Leaders, int Replicas, long SizeBytes);

    private sealed record PartitionMove(
        string Topic,
        int Partition,
        List<int> CurrentReplicas,
        List<int> TargetReplicas);
}
