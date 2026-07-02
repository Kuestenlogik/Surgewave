using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Connect;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Show status for a replication flow (surgewave mirror status)
/// </summary>
public class StatusMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow" };

    private readonly Option<bool> _watchOption = new("--watch", "-w") { Description = "Continuously watch status updates" };

    public StatusMirrorCommand() : base("status", "Show status for a replication flow")
    {
        Arguments.Add(_nameArgument);
        Options.Add(_watchOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArgument);
        var watch = parseResult.GetValue(_watchOption);
        var format = GetFormat(parseResult);
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            do
            {
                var sourceStatus = await client.Connect.GetConnectorStatusAsync($"{name}-source", ct);
                if (sourceStatus == null)
                {
                    WriteError($"Replication flow '{name}' not found (no connector '{name}-source')");
                    return 1;
                }

                var checkpointStatus = await client.Connect.GetConnectorStatusAsync($"{name}-checkpoint", ct);
                var heartbeatStatus = await client.Connect.GetConnectorStatusAsync($"{name}-heartbeat", ct);

                var connectors = new List<ConnectorStatus> { sourceStatus };
                if (checkpointStatus != null) connectors.Add(checkpointStatus);
                if (heartbeatStatus != null) connectors.Add(heartbeatStatus);

                if (format == OutputFormat.Json)
                {
                    var output = new
                    {
                        Name = name,
                        State = sourceStatus.State.ToUpperInvariant(),
                        Connectors = connectors.Select(c => new
                        {
                            c.Name,
                            State = c.State.ToUpperInvariant(),
                            c.WorkerId,
                            Tasks = c.Tasks.Select(t => new { t.Id, State = t.State.ToUpperInvariant(), t.WorkerId, t.Trace }).ToList()
                        }).ToList()
                    };
                    Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
                }
                else if (format == OutputFormat.Plain)
                {
                    foreach (var connector in connectors)
                    {
                        Console.WriteLine($"{connector.Name}\t{connector.State.ToUpperInvariant()}\ttasks={connector.Tasks.Count}");
                        foreach (var task in connector.Tasks.OrderBy(t => t.Id))
                        {
                            Console.WriteLine($"  {task.Id}\t{task.State.ToUpperInvariant()}\t{task.WorkerId}");
                        }
                    }
                }
                else
                {
                    if (watch)
                    {
                        Console.Clear();
                    }

                    var stateColor = GetStatusColor(sourceStatus.State.ToUpperInvariant());
                    AnsiConsole.MarkupLine($"[bold]Mirror: {name}[/]");
                    AnsiConsole.MarkupLine($"State: [{stateColor}]{sourceStatus.State.ToUpperInvariant()}[/]");
                    AnsiConsole.MarkupLine("");

                    var table = new Table();
                    table.AddColumn("Connector");
                    table.AddColumn("Task");
                    table.AddColumn("State");
                    table.AddColumn("Worker");

                    foreach (var connector in connectors)
                    {
                        if (connector.Tasks.Count == 0)
                        {
                            var connectorColor = GetStatusColor(connector.State.ToUpperInvariant());
                            table.AddRow(connector.Name, "—", $"[{connectorColor}]{connector.State.ToUpperInvariant()}[/]", connector.WorkerId);
                            continue;
                        }

                        foreach (var task in connector.Tasks.OrderBy(t => t.Id))
                        {
                            var taskColor = GetStatusColor(task.State.ToUpperInvariant());
                            table.AddRow(
                                connector.Name,
                                task.Id.ToString(),
                                $"[{taskColor}]{task.State.ToUpperInvariant()}[/]",
                                task.WorkerId
                            );
                        }
                    }

                    AnsiConsole.Write(table);

                    var failedTasks = connectors.SelectMany(c => c.Tasks)
                        .Where(t => t.State.Equals("failed", StringComparison.OrdinalIgnoreCase) && t.Trace != null)
                        .ToList();

                    if (failedTasks.Count > 0)
                    {
                        AnsiConsole.MarkupLine("");
                        AnsiConsole.MarkupLine("[red]Failed task traces:[/]");
                        foreach (var task in failedTasks)
                        {
                            AnsiConsole.MarkupLine($"  [red]{task.Id}: {Markup.Escape(task.Trace!)}[/]");
                        }
                    }
                }

                if (watch)
                {
                    // Poll interval for --watch mode
                    await Task.Delay(1000, ct);
                }

            } while (watch && !ct.IsCancellationRequested);
        }
        catch (OperationCanceledException)
        {
            // Normal exit from watch mode
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }

        return 0;
    }

    private static string GetStatusColor(string status) => status switch
    {
        "RUNNING" => "green",
        "PAUSED" => "yellow",
        "FAILED" => "red",
        _ => "dim"
    };
}
