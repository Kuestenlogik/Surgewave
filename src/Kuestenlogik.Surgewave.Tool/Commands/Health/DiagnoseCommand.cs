using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Health;

/// <summary>
/// Comprehensive diagnostic command (surgewave health diagnose)
/// Performs deep health checks and reports issues with recommendations.
/// </summary>
public class DiagnoseCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public DiagnoseCommand() : base("diagnose", "Run comprehensive diagnostics and report issues")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var bootstrapServers = GetBootstrapServers(parseResult);
        var (host, port) = ParseBootstrapServer(bootstrapServers);
        var format = GetFormat(parseResult);

        var checks = new List<DiagnosticCheck>();

        if (format == OutputFormat.Table)
        {
            AnsiConsole.Write(new Rule("[bold blue]Surgewave Health Diagnostics[/]").RuleStyle("blue"));
            AnsiConsole.WriteLine();
        }

        // Run all diagnostic checks
        checks.Add(await CheckTcpConnectivityAsync(host, port, ct));
        checks.Add(await CheckNativeProtocolAsync(host, port, ct));
        checks.Add(await CheckKafkaProtocolAsync(bootstrapServers, ct));
        checks.Add(await CheckTopicsAsync(bootstrapServers, ct));
        checks.Add(await CheckConsumerGroupsAsync(bootstrapServers, ct));
        checks.Add(await CheckLatencyAsync(host, port, ct));

        // Output results
        return OutputResults(format, checks);
    }

    private static async Task<DiagnosticCheck> CheckTcpConnectivityAsync(string host, int port, CancellationToken ct)
    {
        var check = new DiagnosticCheck("TCP Connectivity", $"Connect to {host}:{port}");

        try
        {
            using var client = new TcpClient();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(5000);

            var sw = Stopwatch.StartNew();
            await client.ConnectAsync(host, port, cts.Token);
            sw.Stop();

            check.Status = CheckStatus.Pass;
            check.Message = $"Connected in {sw.ElapsedMilliseconds}ms";
        }
        catch (OperationCanceledException)
        {
            check.Status = CheckStatus.Fail;
            check.Message = "Connection timed out";
            check.Recommendation = "Ensure the broker is running and the port is correct";
        }
        catch (SocketException ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Connection failed: {ex.Message}";
            check.Recommendation = "Check if the broker is running: surgewave broker start";
        }

        return check;
    }

    private static async Task<DiagnosticCheck> CheckNativeProtocolAsync(string host, int port, CancellationToken ct)
    {
        var check = new DiagnosticCheck("Native Protocol", "Surgewave native protocol health");

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(5000);

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(cts.Token);

            var sw = Stopwatch.StartNew();
            var serverTimestamp = await client.Messaging.PingAsync(cts.Token);
            sw.Stop();

            var serverTime = DateTimeOffset.FromUnixTimeMilliseconds(serverTimestamp);
            var drift = Math.Abs((DateTimeOffset.UtcNow - serverTime).TotalSeconds);

            if (drift > 60)
            {
                check.Status = CheckStatus.Warning;
                check.Message = $"Server time drift: {drift:F1}s";
                check.Recommendation = "Synchronize server clock with NTP";
            }
            else
            {
                check.Status = CheckStatus.Pass;
                check.Message = $"Ping: {sw.ElapsedMilliseconds}ms, time drift: {drift:F1}s";
            }
        }
        catch (Exception ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Native protocol error: {ex.Message}";
            check.Recommendation = "The broker may be running in Kafka-only mode";
        }

        return check;
    }

    private static async Task<DiagnosticCheck> CheckKafkaProtocolAsync(string bootstrapServers, CancellationToken ct)
    {
        var check = new DiagnosticCheck("Kafka Protocol", "Kafka wire protocol compatibility");

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(10000);

            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SocketTimeoutMs = 5000
            };

            using var adminClient = new AdminClientBuilder(config).Build();

            var sw = Stopwatch.StartNew();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            sw.Stop();

            var brokerCount = metadata.Brokers.Count;
            var topicCount = metadata.Topics.Count;

            check.Status = CheckStatus.Pass;
            check.Message = $"Metadata in {sw.ElapsedMilliseconds}ms: {brokerCount} broker(s), {topicCount} topic(s)";
        }
        catch (KafkaException ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Kafka protocol error: {ex.Error.Reason}";
            check.Recommendation = "Check broker logs for protocol errors";
        }
        catch (Exception ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Error: {ex.Message}";
        }

        return await Task.FromResult(check);
    }

    private static async Task<DiagnosticCheck> CheckTopicsAsync(string bootstrapServers, CancellationToken ct)
    {
        var check = new DiagnosticCheck("Topics", "Topic health and configuration");

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(10000);

            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SocketTimeoutMs = 5000
            };

            using var adminClient = new AdminClientBuilder(config).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            var issues = new List<string>();
            var totalPartitions = 0;

            foreach (var topic in metadata.Topics)
            {
                if (topic.Error.Code != ErrorCode.NoError)
                {
                    issues.Add($"{topic.Topic}: {topic.Error.Reason}");
                }

                totalPartitions += topic.Partitions.Count;

                foreach (var partition in topic.Partitions)
                {
                    if (partition.Error.Code != ErrorCode.NoError)
                    {
                        issues.Add($"{topic.Topic}[{partition.PartitionId}]: {partition.Error.Reason}");
                    }

                    if (partition.Leader == -1)
                    {
                        issues.Add($"{topic.Topic}[{partition.PartitionId}]: No leader");
                    }
                }
            }

            if (issues.Count > 0)
            {
                check.Status = CheckStatus.Warning;
                check.Message = $"{metadata.Topics.Count} topics, {totalPartitions} partitions, {issues.Count} issue(s)";
                check.Details = issues;
                check.Recommendation = "Review topic configurations and ensure all partitions have leaders";
            }
            else
            {
                check.Status = CheckStatus.Pass;
                check.Message = $"{metadata.Topics.Count} topics, {totalPartitions} partitions - all healthy";
            }
        }
        catch (Exception ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Failed to check topics: {ex.Message}";
        }

        return await Task.FromResult(check);
    }

    private static async Task<DiagnosticCheck> CheckConsumerGroupsAsync(string bootstrapServers, CancellationToken ct)
    {
        var check = new DiagnosticCheck("Consumer Groups", "Consumer group health");

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(10000);

            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SocketTimeoutMs = 5000
            };

            using var adminClient = new AdminClientBuilder(config).Build();
            var groupsResult = await adminClient.ListConsumerGroupsAsync(new ListConsumerGroupsOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(5)
            });

            var groups = groupsResult.Valid.ToList();
            var stableCount = groups.Count(g => g.State == ConsumerGroupState.Stable);
            var emptyCount = groups.Count(g => g.State == ConsumerGroupState.Empty);
            var otherCount = groups.Count - stableCount - emptyCount;

            var issues = new List<string>();

            foreach (var group in groups)
            {
                if (group.State != ConsumerGroupState.Stable && group.State != ConsumerGroupState.Empty)
                {
                    issues.Add($"{group.GroupId}: state={group.State}");
                }
            }

            if (issues.Count > 0)
            {
                check.Status = CheckStatus.Warning;
                check.Message = $"{groups.Count} groups ({stableCount} stable, {emptyCount} empty, {otherCount} other)";
                check.Details = issues;
                check.Recommendation = "Groups in rebalancing state may indicate consumer issues";
            }
            else
            {
                check.Status = CheckStatus.Pass;
                check.Message = $"{groups.Count} groups ({stableCount} stable, {emptyCount} empty)";
            }
        }
        catch (Exception ex)
        {
            check.Status = CheckStatus.Warning;
            check.Message = $"Could not list groups: {ex.Message}";
            check.Recommendation = "This may be expected if no consumer groups exist";
        }

        return check;
    }

    private static async Task<DiagnosticCheck> CheckLatencyAsync(string host, int port, CancellationToken ct)
    {
        var check = new DiagnosticCheck("Latency", "Network latency analysis");

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(10000);

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(cts.Token);

            var latencies = new List<long>();

            // Run 5 ping tests
            for (int i = 0; i < 5; i++)
            {
                var sw = Stopwatch.StartNew();
                await client.Messaging.PingAsync(cts.Token);
                sw.Stop();
                latencies.Add(sw.ElapsedMilliseconds);
                await Task.Delay(100, cts.Token);
            }

            var min = latencies.Min();
            var max = latencies.Max();
            var avg = latencies.Average();
            var p99 = latencies.OrderBy(x => x).ElementAt((int)(latencies.Count * 0.99));

            if (avg > 100)
            {
                check.Status = CheckStatus.Warning;
                check.Message = $"High latency: min={min}ms, avg={avg:F0}ms, max={max}ms";
                check.Recommendation = "Check network connectivity and broker load";
            }
            else
            {
                check.Status = CheckStatus.Pass;
                check.Message = $"min={min}ms, avg={avg:F1}ms, max={max}ms, p99={p99}ms";
            }
        }
        catch (Exception ex)
        {
            check.Status = CheckStatus.Fail;
            check.Message = $"Latency test failed: {ex.Message}";
        }

        return check;
    }

    private static int OutputResults(OutputFormat format, List<DiagnosticCheck> checks)
    {
        var passCount = checks.Count(c => c.Status == CheckStatus.Pass);
        var warnCount = checks.Count(c => c.Status == CheckStatus.Warning);
        var failCount = checks.Count(c => c.Status == CheckStatus.Fail);

        if (format == OutputFormat.Json)
        {
            var result = new
            {
                Summary = new { Pass = passCount, Warning = warnCount, Fail = failCount },
                Checks = checks.Select(c => new
                {
                    c.Name,
                    c.Description,
                    Status = c.Status.ToString().ToLowerInvariant(),
                    c.Message,
                    c.Recommendation,
                    c.Details
                })
            };
            Console.WriteLine(JsonSerializer.Serialize(result, JsonOptions));
        }
        else if (format == OutputFormat.Plain)
        {
            foreach (var check in checks)
            {
                var status = check.Status switch
                {
                    CheckStatus.Pass => "PASS",
                    CheckStatus.Warning => "WARN",
                    CheckStatus.Fail => "FAIL",
                    _ => "UNKNOWN"
                };
                Console.WriteLine($"{status} {check.Name}: {check.Message}");
                if (check.Recommendation != null)
                {
                    Console.WriteLine($"     Recommendation: {check.Recommendation}");
                }
            }
            Console.WriteLine();
            Console.WriteLine($"Summary: {passCount} passed, {warnCount} warnings, {failCount} failed");
        }
        else
        {
            var table = new Table()
                .Border(TableBorder.Rounded)
                .AddColumn("Check")
                .AddColumn("Status")
                .AddColumn("Details");

            foreach (var check in checks)
            {
                var statusMarkup = check.Status switch
                {
                    CheckStatus.Pass => "[green]PASS[/]",
                    CheckStatus.Warning => "[yellow]WARN[/]",
                    CheckStatus.Fail => "[red]FAIL[/]",
                    _ => "[dim]?[/]"
                };

                var details = check.Message ?? "";
                if (check.Recommendation != null)
                {
                    details += $"\n[dim italic]{check.Recommendation}[/]";
                }

                table.AddRow(
                    new Markup($"[bold]{check.Name}[/]\n[dim]{check.Description}[/]"),
                    new Markup(statusMarkup),
                    new Markup(details)
                );
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();

            // Summary
            var summaryColor = failCount > 0 ? "red" : (warnCount > 0 ? "yellow" : "green");
            AnsiConsole.MarkupLine($"[{summaryColor}]Summary: {passCount} passed, {warnCount} warnings, {failCount} failed[/]");

            if (failCount > 0)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[red]Some checks failed. Review the recommendations above.[/]");
            }
        }

        return failCount > 0 ? 1 : 0;
    }

    private sealed class DiagnosticCheck(string name, string description)
    {
        public string Name { get; } = name;
        public string Description { get; } = description;
        public CheckStatus Status { get; set; } = CheckStatus.Unknown;
        public string? Message { get; set; }
        public string? Recommendation { get; set; }
        public List<string>? Details { get; set; }
    }

    private enum CheckStatus
    {
        Unknown,
        Pass,
        Warning,
        Fail
    }
}
