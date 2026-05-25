using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Partitions;

/// <summary>
/// Show the status of a reassignment plan (surgewave partitions reassign-status).
/// </summary>
public class ReassignStatusCommand : CommandBase
{
    private readonly Option<string?> _planIdOpt = new("--plan-id") { Description = "Reassignment plan ID (omit to list all)" };

    public ReassignStatusCommand() : base("reassign-status", "Show reassignment plan status and progress")
    {
        Options.Add(_planIdOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var planId = parseResult.GetValue(_planIdOpt);

        try
        {
            using var http = new HttpClient { BaseAddress = new Uri($"http://{host}:{port}") };

            if (!string.IsNullOrEmpty(planId))
            {
                return await ShowPlanStatus(http, planId, format, ct);
            }
            else
            {
                return await ListAllPlans(http, format, ct);
            }
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get reassignment status: {ex.Message}");
            return 1;
        }
    }

    private async Task<int> ShowPlanStatus(HttpClient http, string planId, OutputFormat format, CancellationToken ct)
    {
        var response = await http.GetAsync($"/api/partitions/reassign/{planId}", ct);
        var json = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            WriteError($"Plan not found: {response.StatusCode}");
            return 1;
        }

        if (format == OutputFormat.Json)
        {
            Console.WriteLine(json);
            return 0;
        }

        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        AnsiConsole.MarkupLine($"[bold]Plan:[/] {root.GetProperty("planId").GetString()}");
        AnsiConsole.MarkupLine($"[bold]Status:[/] {root.GetProperty("status").GetString()}");

        if (root.TryGetProperty("description", out var desc) && desc.ValueKind == JsonValueKind.String)
            AnsiConsole.MarkupLine($"[bold]Description:[/] {desc.GetString()}");

        AnsiConsole.MarkupLine($"[bold]Completed:[/] {root.GetProperty("completed").GetInt32()}, [bold]Failed:[/] {root.GetProperty("failed").GetInt32()}");
        AnsiConsole.WriteLine();

        var assignments = root.GetProperty("assignments");

        if (assignments.GetArrayLength() > 0)
        {
            var table = new Table();
            table.AddColumn("Topic");
            table.AddColumn("Partition");
            table.AddColumn("Status");
            table.AddColumn("Progress");
            table.AddColumn("Bytes Copied");
            table.AddColumn("Error");

            foreach (var a in assignments.EnumerateArray())
            {
                var status = a.GetProperty("status").GetString() ?? "";
                var statusColor = status switch
                {
                    "Completed" => "green",
                    "Failed" or "Cancelled" => "red",
                    "Syncing" => "yellow",
                    _ => "blue"
                };

                var progress = a.GetProperty("progress").GetDouble();
                var bytesCopied = a.GetProperty("bytesCopied").GetInt64();
                var totalBytes = a.GetProperty("totalBytes").GetInt64();
                var error = a.TryGetProperty("error", out var errEl) && errEl.ValueKind == JsonValueKind.String
                    ? errEl.GetString() : "";

                table.AddRow(
                    a.GetProperty("topic").GetString() ?? "",
                    a.GetProperty("partition").GetInt32().ToString(),
                    $"[{statusColor}]{status}[/]",
                    $"{progress * 100:F1}%",
                    totalBytes > 0 ? $"{bytesCopied}/{totalBytes}" : "—",
                    error ?? "");
            }

            AnsiConsole.Write(table);
        }

        return 0;
    }

    private async Task<int> ListAllPlans(HttpClient http, OutputFormat format, CancellationToken ct)
    {
        var response = await http.GetAsync("/api/partitions/reassign", ct);
        var json = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            WriteError($"Failed to list plans: {response.StatusCode}");
            return 1;
        }

        if (format == OutputFormat.Json)
        {
            Console.WriteLine(json);
            return 0;
        }

        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (root.GetArrayLength() == 0)
        {
            AnsiConsole.MarkupLine("[dim]No reassignment plans found.[/]");
            return 0;
        }

        AnsiConsole.MarkupLine("[bold]Reassignment Plans[/]");
        AnsiConsole.WriteLine();

        var table = new Table();
        table.AddColumn("Plan ID");
        table.AddColumn("Status");
        table.AddColumn("Partitions");
        table.AddColumn("Completed");
        table.AddColumn("Failed");
        table.AddColumn("Created");
        table.AddColumn("Description");

        foreach (var p in root.EnumerateArray())
        {
            var statusStr = p.GetProperty("status").GetString() ?? "";
            var statusColor = statusStr switch
            {
                "Completed" => "green",
                "Failed" or "Cancelled" => "red",
                "Executing" => "yellow",
                _ => "blue"
            };

            table.AddRow(
                p.GetProperty("planId").GetString() ?? "",
                $"[{statusColor}]{statusStr}[/]",
                p.GetProperty("totalPartitions").GetInt32().ToString(),
                p.GetProperty("completed").GetInt32().ToString(),
                p.GetProperty("failed").GetInt32().ToString(),
                p.GetProperty("createdAt").GetString() ?? "",
                p.TryGetProperty("description", out var d) && d.ValueKind == JsonValueKind.String
                    ? d.GetString() ?? "" : "");
        }

        AnsiConsole.Write(table);
        return 0;
    }
}
