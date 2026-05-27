using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Broker;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for broker information (surgewave broker ...)
/// </summary>
public class BrokerCommand : CommandBase
{
    public BrokerCommand() : base("broker", "Broker information and health")
    {
        Subcommands.Add(new BrokerInfoCommand());
        Subcommands.Add(new BrokerHealthCommand());
        Subcommands.Add(new BrokerConfigCommand());
    }
}

/// <summary>
/// Get broker info (surgewave broker info)
/// </summary>
public class BrokerInfoCommand : CommandBase
{
    public BrokerInfoCommand() : base("info", "Display broker information")
    {
        Aliases.Add("describe");
        Aliases.Add("show");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            var topics = await client.Topics.ListAsync(ct);
            var totalPartitions = topics.Sum(t => t.PartitionCount);

            if (format == OutputFormat.Json)
            {
                var info = new
                {
                    Broker = new { Host = host, Port = port },
                    TopicCount = topics.Count,
                    TotalPartitions = totalPartitions
                };
                Console.WriteLine(JsonSerializer.Serialize(info, JsonOptions.Indented));
            }
            else if (Console.IsOutputRedirected() || Environment.GetEnvironmentVariable("NO_COLOR") != null)
            {
                // Plain text output for piped/redirected scenarios or when NO_COLOR is set
                Console.WriteLine("Surgewave Broker Info");
                Console.WriteLine($"Broker: {host}:{port}");
                Console.WriteLine("Protocol: Surgewave Native");
                Console.WriteLine($"Topics: {topics.Count}");
                Console.WriteLine($"Total Partitions: {totalPartitions}");
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Surgewave Broker Info[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Broker:[/]", $"{host}:{port}");
                grid.AddRow("[bold]Protocol:[/]", "Surgewave Native");
                grid.AddRow("[bold]Topics:[/]", topics.Count.ToString());
                grid.AddRow("[bold]Total Partitions:[/]", totalPartitions.ToString());

                AnsiConsole.Write(grid);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get broker info: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>
/// Check broker health (surgewave broker health)
/// </summary>
public class BrokerHealthCommand : CommandBase
{
    public BrokerHealthCommand() : base("health", "Check broker health")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var exitCode = 0;

        await AnsiConsole.Status()
            .StartAsync("Checking broker health...", async ctx =>
            {
                try
                {
                    var startTime = DateTime.UtcNow;
                    await using var client = new SurgewaveNativeClient(host, port);
                    await client.ConnectAsync(ct);
                    var serverTimestamp = await client.Messaging.PingAsync(ct);
                    var responseTime = (DateTime.UtcNow - startTime).TotalMilliseconds;

                    var topics = await client.Topics.ListAsync(ct);

                    if (format == OutputFormat.Json)
                    {
                        var health = new
                        {
                            Status = "healthy",
                            ResponseTimeMs = responseTime,
                            ServerTimestamp = serverTimestamp,
                            TopicCount = topics.Count
                        };
                        Console.WriteLine(JsonSerializer.Serialize(health, JsonOptions.Indented));
                    }
                    else
                    {
                        AnsiConsole.MarkupLine($"[green]Status:[/] healthy");
                        AnsiConsole.MarkupLine($"[green]Response time:[/] {responseTime:F0}ms");
                        AnsiConsole.MarkupLine($"[green]Topics:[/] {topics.Count}");
                    }
                }
                catch (Exception ex)
                {
                    if (format == OutputFormat.Json)
                    {
                        var health = new { Status = "unhealthy", Error = ex.Message };
                        Console.WriteLine(JsonSerializer.Serialize(health, JsonOptions.Indented));
                    }
                    else
                    {
                        AnsiConsole.MarkupLine($"[red]Status:[/] unhealthy");
                        AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
                    }
                    exitCode = 1;
                }
            });

        return exitCode;
    }
}
