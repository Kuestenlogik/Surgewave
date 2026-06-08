using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Partitions;

/// <summary>
/// Show current partition assignment across all brokers (surgewave partitions assignment).
/// </summary>
public class AssignmentCommand : CommandBase
{
    public AssignmentCommand() : base("assignment", "Show current partition assignment across all brokers")
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
            var response = await http.GetAsync("/api/partitions/assignment", ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to get assignment: {response.StatusCode} — {json}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
            }
            else if (format == OutputFormat.Plain)
            {
                var doc = JsonDocument.Parse(json);
                foreach (var a in doc.RootElement.EnumerateArray())
                {
                    var replicas = string.Join(",", a.GetProperty("replicas").EnumerateArray().Select(r => r.GetInt32()));
                    var isr = string.Join(",", a.GetProperty("isr").EnumerateArray().Select(r => r.GetInt32()));
                    Console.WriteLine($"{a.GetProperty("topic").GetString()}\t{a.GetProperty("partition").GetInt32()}\t{a.GetProperty("leader").GetInt32()}\t{replicas}\t{isr}");
                }
            }
            else
            {
                var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.GetArrayLength() == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No partitions assigned.[/]");
                    return 0;
                }

                AnsiConsole.MarkupLine("[bold]Partition Assignment[/]");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Topic");
                table.AddColumn("Partition");
                table.AddColumn("Leader");
                table.AddColumn("Replicas");
                table.AddColumn("ISR");

                foreach (var a in root.EnumerateArray())
                {
                    var replicas = new List<string>();
                    foreach (var r in a.GetProperty("replicas").EnumerateArray())
                        replicas.Add(r.GetInt32().ToString());

                    var isr = new List<string>();
                    foreach (var r in a.GetProperty("isr").EnumerateArray())
                        isr.Add(r.GetInt32().ToString());

                    table.AddRow(
                        a.GetProperty("topic").GetString() ?? "",
                        a.GetProperty("partition").GetInt32().ToString(),
                        a.GetProperty("leader").GetInt32().ToString(),
                        string.Join(",", replicas),
                        string.Join(",", isr));
                }

                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get partition assignment: {ex.Message}");
            return 1;
        }
    }
}
