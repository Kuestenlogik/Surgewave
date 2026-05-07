using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Show status and lag for a replication flow (surgewave mirror status)
/// </summary>
public class StatusMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow" };

    private readonly Option<bool> _watchOption = new("--watch", "-w") { Description = "Continuously watch status updates" };

    public StatusMirrorCommand() : base("status", "Show status and lag for a replication flow")
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

        try
        {
            do
            {
                // In a real implementation, this would query actual status
                var status = new
                {
                    Name = name,
                    State = "RUNNING",
                    Tasks = new[]
                    {
                        new { Id = 0, State = "RUNNING", Topic = "orders", Partition = 0, Lag = 10L },
                        new { Id = 0, State = "RUNNING", Topic = "orders", Partition = 1, Lag = 5L },
                        new { Id = 1, State = "RUNNING", Topic = "payments", Partition = 0, Lag = 0L },
                        new { Id = 1, State = "RUNNING", Topic = "payments", Partition = 1, Lag = 2L },
                    },
                    TotalLag = 17L,
                    RecordsPerSecond = 1250.5,
                    BytesPerSecond = 524288L,
                    HealthStatus = "HEALTHY",
                    LastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-2)
                };

                if (format == OutputFormat.Json)
                {
                    Console.WriteLine(JsonSerializer.Serialize(status, JsonOptions.Indented));
                }
                else if (format == OutputFormat.Plain)
                {
                    Console.WriteLine($"{status.Name}\t{status.State}\tlag={status.TotalLag}\t{status.RecordsPerSecond:F0} rec/s");
                }
                else
                {
                    if (watch)
                    {
                        Console.Clear();
                    }

                    var healthColor = GetStatusColor(status.HealthStatus);
                    var stateColor = GetStatusColor(status.State);

                    AnsiConsole.MarkupLine($"[bold]Mirror: {name}[/]");
                    AnsiConsole.MarkupLine($"State: [{stateColor}]{status.State}[/]  Health: [{healthColor}]{status.HealthStatus}[/]");
                    AnsiConsole.MarkupLine($"Last Heartbeat: {status.LastHeartbeat:HH:mm:ss}");
                    AnsiConsole.MarkupLine("");

                    // Summary metrics
                    var panel = new Panel(new Markup(
                        $"[bold]Total Lag:[/] {status.TotalLag} records\n" +
                        $"[bold]Throughput:[/] {status.RecordsPerSecond:F0} rec/s ({FormatBytes(status.BytesPerSecond)}/s)"))
                    {
                        Border = BoxBorder.Rounded,
                        Header = new PanelHeader("Metrics")
                    };
                    AnsiConsole.Write(panel);
                    AnsiConsole.MarkupLine("");

                    // Task status
                    var table = new Table();
                    table.AddColumn("Task");
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("Lag");
                    table.AddColumn("State");

                    foreach (var task in status.Tasks)
                    {
                        var lagColor = task.Lag == 0 ? "green" : task.Lag < 100 ? "yellow" : "red";
                        var taskStateColor = task.State == "RUNNING" ? "green" : "red";
                        table.AddRow(
                            task.Id.ToString(),
                            task.Topic,
                            task.Partition.ToString(),
                            $"[{lagColor}]{task.Lag}[/]",
                            $"[{taskStateColor}]{task.State}[/]"
                        );
                    }

                    AnsiConsole.Write(table);
                }

                if (watch)
                {
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

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB"];
        int index = 0;
        double size = bytes;

        while (size >= 1024 && index < suffixes.Length - 1)
        {
            size /= 1024;
            index++;
        }

        return $"{size:F1} {suffixes[index]}";
    }

    private static string GetStatusColor(string status) => status switch
    {
        "RUNNING" or "HEALTHY" => "green",
        "PAUSED" or "WARNING" => "yellow",
        "FAILED" or "UNHEALTHY" => "red",
        _ => "dim"
    };
}
