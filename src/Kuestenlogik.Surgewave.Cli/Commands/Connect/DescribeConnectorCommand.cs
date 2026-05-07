using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Describe a connector (surgewave connect describe)
/// </summary>
public class DescribeConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public DescribeConnectorCommand() : base("describe", "Describe a connector")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Describing connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var info = await client.Connect.GetConnectorAsync(name, ct);
            if (info == null)
            {
                WriteError($"Connector '{name}' not found");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    info.Name,
                    info.Type,
                    info.State,
                    info.WorkerId,
                    info.Config,
                    Tasks = info.Tasks.Select(t => new { t.Id, t.State, t.WorkerId }).ToList()
                }, ConnectJsonOptions.Indented));
            }
            else
            {
                var state = info.State.ToUpperInvariant() switch
                {
                    "RUNNING" => "[green]Running[/]",
                    "PAUSED" => "[yellow]Paused[/]",
                    "FAILED" => "[red]Failed[/]",
                    _ => info.State
                };

                AnsiConsole.MarkupLine($"[bold]Name:[/] {info.Name}");
                AnsiConsole.MarkupLine($"[bold]Type:[/] {info.Type}");
                AnsiConsole.MarkupLine($"[bold]State:[/] {state}");
                AnsiConsole.MarkupLine($"[bold]Worker:[/] {info.WorkerId}");
                AnsiConsole.WriteLine();

                AnsiConsole.MarkupLine("[bold]Configuration:[/]");
                var configTable = new Table().NoBorder();
                configTable.AddColumn("Key");
                configTable.AddColumn("Value");
                foreach (var (key, value) in info.Config.OrderBy(kv => kv.Key))
                {
                    configTable.AddRow(key, value ?? "[dim]null[/]");
                }
                AnsiConsole.Write(configTable);
                AnsiConsole.WriteLine();

                AnsiConsole.MarkupLine("[bold]Tasks:[/]");
                var taskTable = new Table();
                taskTable.AddColumn("ID");
                taskTable.AddColumn("State");
                taskTable.AddColumn("Worker");
                foreach (var task in info.Tasks.OrderBy(t => t.Id))
                {
                    var taskState = task.State.ToUpperInvariant() switch
                    {
                        "RUNNING" => "[green]Running[/]",
                        "PAUSED" => "[yellow]Paused[/]",
                        "FAILED" => "[red]Failed[/]",
                        _ => task.State
                    };
                    taskTable.AddRow(task.Id.ToString(), taskState, task.WorkerId);
                }
                AnsiConsole.Write(taskTable);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe connector: {ex.Message}");
            return 1;
        }
    }
}
