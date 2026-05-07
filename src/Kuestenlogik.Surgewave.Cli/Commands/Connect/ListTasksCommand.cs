using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// List tasks for a connector (surgewave connect tasks list)
/// </summary>
public class ListTasksCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("connector") { Description = "Connector name" };

    public ListTasksCommand() : base("list", "List tasks for a connector")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Listing tasks for connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var tasks = await client.Connect.GetConnectorTasksAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(tasks.Select(t => new { t.Connector, t.TaskId }).ToList(), ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var task in tasks)
                {
                    Console.WriteLine($"{task.Connector}/{task.TaskId}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Connector");
                table.AddColumn("Task ID");

                foreach (var task in tasks.OrderBy(t => t.TaskId))
                {
                    table.AddRow(task.Connector, task.TaskId.ToString());
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {tasks.Count} task(s)[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list tasks: {ex.Message}");
            return 1;
        }
    }
}
