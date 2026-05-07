using System.CommandLine;
using System.CommandLine.Parsing;
using System.Net.Http.Json;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Partitions;

/// <summary>
/// Auto-balance partitions across all brokers (surgewave partitions balance).
/// </summary>
public class BalancePartitionsCommand : CommandBase
{
    public BalancePartitionsCommand() : base("balance", "Auto-balance partitions across all brokers")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            using var http = new HttpClient { BaseAddress = new Uri($"http://{host}:{port}") };
            var response = await http.PostAsync("/api/partitions/balance", null, ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Balance failed: {response.StatusCode} — {json}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
            }
            else
            {
                var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                var planId = root.GetProperty("planId").GetString();
                var status = root.GetProperty("status").GetString();
                var partitions = root.GetProperty("assignments");

                if (partitions.GetArrayLength() == 0)
                {
                    AnsiConsole.MarkupLine("[green]Cluster is already balanced. No reassignments needed.[/]");
                }
                else
                {
                    WriteSuccess($"Balance plan {planId} submitted ({status})");

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("Current");
                    table.AddColumn("Target");

                    foreach (var a in partitions.EnumerateArray())
                    {
                        table.AddRow(
                            a.GetProperty("topic").GetString() ?? "",
                            a.GetProperty("partition").GetInt32().ToString(),
                            FormatReplicas(a.GetProperty("currentReplicas")),
                            FormatReplicas(a.GetProperty("targetReplicas")));
                    }

                    AnsiConsole.Write(table);
                    AnsiConsole.MarkupLine($"\n[dim]Use 'surgewave partitions reassign-status --plan-id {planId}' to check progress.[/]");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to balance partitions: {ex.Message}");
            return 1;
        }
    }

    private static string FormatReplicas(JsonElement element)
    {
        var ids = new List<string>();
        foreach (var item in element.EnumerateArray())
            ids.Add(item.GetInt32().ToString());
        return string.Join(",", ids);
    }
}
