using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Resume a paused replication flow (surgewave mirror resume)
/// </summary>
public class ResumeMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow to resume" };

    public ResumeMirrorCommand() : base("resume", "Resume a paused replication flow")
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
            WriteVerbose(parseResult, $"Resuming replication flow '{name}'...");

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Resume all mirror connectors (source, checkpoint, heartbeat)
            var connectorNames = new[] { $"{name}-source", $"{name}-checkpoint", $"{name}-heartbeat" };
            var resumedCount = 0;

            foreach (var connectorName in connectorNames)
            {
                try
                {
                    await client.Connect.ResumeConnectorAsync(connectorName, ct);
                    WriteVerbose(parseResult, $"  Resumed connector '{connectorName}'");
                    resumedCount++;
                }
                catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase))
                {
                    // Connector doesn't exist (e.g., heartbeat disabled), skip it
                    WriteVerbose(parseResult, $"  Connector '{connectorName}' not found, skipping");
                }
            }

            if (resumedCount == 0)
            {
                WriteError($"No connectors found for replication flow '{name}'");
                return 1;
            }

            WriteSuccess($"Replication flow '{name}' resumed successfully ({resumedCount} connector(s)).");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to resume replication flow: {ex.Message}");
            return 1;
        }
    }
}
