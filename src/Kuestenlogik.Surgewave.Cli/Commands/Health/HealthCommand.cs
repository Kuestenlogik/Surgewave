using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Health;

/// <summary>
/// Top-level health check command (surgewave health)
/// Quick broker health check for monitoring and scripting.
/// </summary>
public class HealthCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<bool> _quietOpt = new("--quiet", "-q") { Description = "Only output status code (0=healthy, 1=unhealthy)", DefaultValueFactory = _ => false };
    private readonly Option<int> _timeoutOpt = new("--timeout", "-t") { Description = "Connection timeout in milliseconds", DefaultValueFactory = _ => 5000 };

    public HealthCommand() : base("health", "Check broker health (quick health check)")
    {
        Options.Add(_quietOpt);
        Options.Add(_timeoutOpt);
        Subcommands.Add(new DiagnoseCommand());
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var quiet = parseResult.GetValue(_quietOpt);
        var timeout = parseResult.GetValue(_timeoutOpt);

        var sw = Stopwatch.StartNew();

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(cts.Token);

            var serverTimestamp = await client.Messaging.PingAsync(cts.Token);
            sw.Stop();

            var topics = await client.Topics.ListAsync(cts.Token);
            var totalPartitions = topics.Sum(t => t.PartitionCount);

            if (quiet)
            {
                return 0;
            }

            if (format == OutputFormat.Json)
            {
                var result = new
                {
                    Status = "healthy",
                    Broker = $"{host}:{port}",
                    ResponseTimeMs = sw.ElapsedMilliseconds,
                    ServerTimestamp = serverTimestamp,
                    Topics = topics.Count,
                    Partitions = totalPartitions
                };
                Console.WriteLine(JsonSerializer.Serialize(result, JsonOptions));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"healthy {host}:{port} {sw.ElapsedMilliseconds}ms {topics.Count} topics");
            }
            else
            {
                var panel = new Panel(
                    new Markup($"[green bold]HEALTHY[/]\n\n" +
                               $"[dim]Broker:[/] {host}:{port}\n" +
                               $"[dim]Response:[/] {sw.ElapsedMilliseconds}ms\n" +
                               $"[dim]Topics:[/] {topics.Count}\n" +
                               $"[dim]Partitions:[/] {totalPartitions}"))
                {
                    Border = BoxBorder.Rounded,
                    Padding = new Padding(2, 1)
                };
                AnsiConsole.Write(panel);
            }

            return 0;
        }
        catch (OperationCanceledException)
        {
            return OutputUnhealthy(format, quiet, host, port, "Connection timeout");
        }
        catch (Exception ex)
        {
            return OutputUnhealthy(format, quiet, host, port, ex.Message);
        }
    }

    private static int OutputUnhealthy(OutputFormat format, bool quiet, string host, int port, string error)
    {
        if (quiet)
        {
            return 1;
        }

        if (format == OutputFormat.Json)
        {
            var result = new
            {
                Status = "unhealthy",
                Broker = $"{host}:{port}",
                Error = error
            };
            Console.WriteLine(JsonSerializer.Serialize(result, JsonOptions));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"unhealthy {host}:{port} {error}");
        }
        else
        {
            var panel = new Panel(
                new Markup($"[red bold]UNHEALTHY[/]\n\n" +
                           $"[dim]Broker:[/] {host}:{port}\n" +
                           $"[red]Error:[/] {error}"))
            {
                Border = BoxBorder.Rounded,
                Padding = new Padding(2, 1)
            };
            AnsiConsole.Write(panel);
        }

        return 1;
    }
}
