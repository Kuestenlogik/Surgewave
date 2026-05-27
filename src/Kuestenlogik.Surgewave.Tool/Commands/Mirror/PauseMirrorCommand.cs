using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Pause a replication flow (surgewave mirror pause)
/// </summary>
public class PauseMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow to pause" };

    public PauseMirrorCommand() : base("pause", "Pause a replication flow")
    {
        Arguments.Add(_nameArgument);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArgument);
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));

        try
        {
            WriteVerbose(parseResult, $"Pausing replication flow '{name}'...");

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Pause all mirror connectors (source, checkpoint, heartbeat)
            var connectorNames = new[] { $"{name}-source", $"{name}-checkpoint", $"{name}-heartbeat" };
            var pausedCount = 0;

            foreach (var connectorName in connectorNames)
            {
                try
                {
                    await client.Connect.PauseConnectorAsync(connectorName, ct);
                    WriteVerbose(parseResult, $"  Paused connector '{connectorName}'");
                    pausedCount++;
                }
                catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase))
                {
                    // Connector doesn't exist (e.g., heartbeat disabled), skip it
                    WriteVerbose(parseResult, $"  Connector '{connectorName}' not found, skipping");
                }
            }

            if (pausedCount == 0)
            {
                WriteError($"No connectors found for replication flow '{name}'");
                return 1;
            }

            WriteSuccess($"Replication flow '{name}' paused successfully ({pausedCount} connector(s)).");
            AnsiConsole.MarkupLine("[dim]Use 'surgewave mirror resume' to resume replication.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to pause replication flow: {ex.Message}");
            return 1;
        }
    }
}
