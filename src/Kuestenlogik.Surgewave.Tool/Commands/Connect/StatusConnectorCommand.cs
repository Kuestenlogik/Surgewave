using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Get connector status (surgewave connect status)
/// </summary>
public class StatusConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public StatusConnectorCommand() : base("status", "Get connector status")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Getting status for connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var status = await client.Connect.GetConnectorStatusAsync(name, ct);
            if (status == null)
            {
                WriteError($"Connector '{name}' not found");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    status.Name,
                    Connector = new { status.State, status.WorkerId },
                    Tasks = status.Tasks.Select(t => new { t.Id, t.State, t.WorkerId, t.Trace }).ToList()
                }, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var task in status.Tasks.OrderBy(t => t.Id))
                    Console.WriteLine($"{task.Id}\t{task.State}\t{task.WorkerId}");
            }
            else
            {
                var state = status.State.ToUpperInvariant() switch
                {
                    "RUNNING" => "[green]Running[/]",
                    "PAUSED" => "[yellow]Paused[/]",
                    "FAILED" => "[red]Failed[/]",
                    _ => status.State
                };

                AnsiConsole.MarkupLine($"[bold]Connector:[/] {status.Name}");
                AnsiConsole.MarkupLine($"[bold]State:[/] {state}");
                AnsiConsole.MarkupLine($"[bold]Worker:[/] {status.WorkerId}");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Task ID");
                table.AddColumn("State");
                table.AddColumn("Worker");

                foreach (var task in status.Tasks.OrderBy(t => t.Id))
                {
                    var taskState = task.State.ToUpperInvariant() switch
                    {
                        "RUNNING" => "[green]Running[/]",
                        "PAUSED" => "[yellow]Paused[/]",
                        "FAILED" => "[red]Failed[/]",
                        _ => task.State
                    };
                    table.AddRow(task.Id.ToString(), taskState, task.WorkerId);
                }
                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get connector status: {ex.Message}");
            return 1;
        }
    }
}
