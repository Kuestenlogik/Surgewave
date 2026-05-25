using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Transport;

/// <summary>
/// Transport status command (surgewave transport status)
/// </summary>
public class TransportStatusCommand : CommandBase
{
    public TransportStatusCommand() : base("status", "Show transport connection status")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            // Test TCP connection
            var tcpStatus = await TestTcpConnectionAsync(host, port, ct);

            // Test Shared Memory connection (if available)
            var shmStatus = TestSharedMemoryConnection(host, port);

            if (format == OutputFormat.Json)
            {
                var status = new
                {
                    Tcp = new
                    {
                        Available = tcpStatus.Connected,
                        Host = host,
                        Port = port,
                        Latency = tcpStatus.LatencyMs,
                        Error = tcpStatus.Error
                    },
                    SharedMemory = new
                    {
                        Available = shmStatus.Available,
                        Path = shmStatus.Path,
                        Enabled = shmStatus.Enabled,
                        Error = shmStatus.Error
                    }
                };
                Console.WriteLine(JsonSerializer.Serialize(status, TransportJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Transport Status[/]").LeftJustified());
                AnsiConsole.WriteLine();

                // TCP Status
                var tcpPanel = new Panel(BuildTcpStatus(tcpStatus, host, port))
                {
                    Header = new PanelHeader("[bold]TCP Transport[/]"),
                    Border = BoxBorder.Rounded
                };
                AnsiConsole.Write(tcpPanel);
                AnsiConsole.WriteLine();

                // Shared Memory Status
                var shmPanel = new Panel(BuildShmStatus(shmStatus))
                {
                    Header = new PanelHeader("[bold]Shared Memory Transport[/]"),
                    Border = BoxBorder.Rounded
                };
                AnsiConsole.Write(shmPanel);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get transport status: {ex.Message}");
            return 1;
        }
    }

    private static async Task<TcpConnectionStatus> TestTcpConnectionAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(cancellationToken);
            await client.Messaging.PingAsync(cancellationToken);
            sw.Stop();

            return new TcpConnectionStatus
            {
                Connected = true,
                LatencyMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            return new TcpConnectionStatus
            {
                Connected = false,
                Error = ex.Message
            };
        }
    }

    // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
    // SharedMemory diagnostics require the Kuestenlogik.Surgewave.Transport.SharedMemory package.
    private static ShmConnectionStatus TestSharedMemoryConnection(string host, int port)
    {
        return new ShmConnectionStatus
        {
            Available = false,
            Enabled = false,
            Path = $"surgewave-broker-{port}",
            Error = "SharedMemory transport plugin not installed."
        };
    }

    private static string BuildTcpStatus(TcpConnectionStatus status, string host, int port)
    {
        var sb = new System.Text.StringBuilder();

        if (status.Connected)
        {
            sb.AppendLine($"[green]Connected[/] to {host}:{port}");
            sb.AppendLine($"Latency: {status.LatencyMs}ms");
        }
        else
        {
            sb.AppendLine($"[red]Not Connected[/]");
            sb.AppendLine($"Target: {host}:{port}");
            if (!string.IsNullOrEmpty(status.Error))
            {
                sb.AppendLine($"[dim]Error: {status.Error}[/]");
            }
        }

        return sb.ToString().TrimEnd();
    }

    private static string BuildShmStatus(ShmConnectionStatus status)
    {
        var sb = new System.Text.StringBuilder();

        if (status.Available)
        {
            sb.AppendLine($"[green]Available[/]");
            sb.AppendLine($"Path: {status.Path}");
            sb.AppendLine($"Active Clients: {status.ClientCount}");
        }
        else
        {
            sb.AppendLine($"[yellow]Not Available[/]");
            sb.AppendLine($"Expected Path: {status.Path}");
            if (!string.IsNullOrEmpty(status.Error))
            {
                sb.AppendLine($"[dim]{status.Error}[/]");
            }
        }

        return sb.ToString().TrimEnd();
    }

    private sealed class TcpConnectionStatus
    {
        public bool Connected { get; init; }
        public long LatencyMs { get; init; }
        public string? Error { get; init; }
    }

    private sealed class ShmConnectionStatus
    {
        public bool Available { get; init; }
        public bool Enabled { get; init; }
        public string? Path { get; init; }
        public int ClientCount { get; init; }
        public string? Error { get; init; }
    }
}

/// <summary>
/// JSON serialization options for transport commands
/// </summary>
internal static class TransportJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
}
