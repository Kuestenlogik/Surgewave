using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Partitions;

/// <summary>
/// Decommission a broker by moving all its partitions elsewhere (surgewave partitions decommission).
/// </summary>
public class DecommissionCommand : CommandBase
{
    private readonly Option<int> _brokerOpt = new("--broker", "-b")
    {
        Description = "Broker ID to decommission",
        Required = true
    };

    public DecommissionCommand() : base("decommission", "Move all partitions off a broker for decommission")
    {
        Options.Add(_brokerOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var brokerId = parseResult.GetValue(_brokerOpt);

        if (!ConfirmDestructive(parseResult, $"Decommission broker [cyan]{brokerId}[/]? All its partitions will move to other brokers."))
        {
            WriteWarning("Decommission cancelled.");
            return 0;
        }

        try
        {
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.PostAsync($"/api/partitions/decommission/{brokerId}", null, ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Decommission failed: {response.StatusCode} — {json}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
            }
            else if (format == OutputFormat.Plain)
            {
                var planId = JsonDocument.Parse(json).RootElement.GetProperty("planId").GetString();
                Console.WriteLine($"plan {planId} broker={brokerId}");
            }
            else
            {
                var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                var planId = root.GetProperty("planId").GetString();
                var assignments = root.GetProperty("assignments");

                if (assignments.GetArrayLength() == 0)
                {
                    AnsiConsole.MarkupLine($"[green]Broker {brokerId} has no partitions to move.[/]");
                }
                else
                {
                    WriteSuccess($"Decommission plan {planId} submitted for broker {brokerId}");
                    AnsiConsole.MarkupLine($"[dim]{assignments.GetArrayLength()} partition(s) will be moved.[/]");

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("Target Replicas");

                    foreach (var a in assignments.EnumerateArray())
                    {
                        var targetReplicas = new List<string>();
                        foreach (var r in a.GetProperty("targetReplicas").EnumerateArray())
                            targetReplicas.Add(r.GetInt32().ToString());

                        table.AddRow(
                            a.GetProperty("topic").GetString() ?? "",
                            a.GetProperty("partition").GetInt32().ToString(),
                            string.Join(",", targetReplicas));
                    }

                    AnsiConsole.Write(table);
                    AnsiConsole.MarkupLine($"\n[dim]Use 'surgewave partitions reassign-status --plan-id {planId}' to check progress.[/]");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to decommission broker: {ex.Message}");
            return 1;
        }
    }
}
