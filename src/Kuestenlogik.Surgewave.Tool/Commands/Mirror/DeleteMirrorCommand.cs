using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Delete a replication flow (surgewave mirror delete)
/// </summary>
public class DeleteMirrorCommand : CommandBase
{
    private readonly Argument<string> _nameArgument = new("name") { Description = "Name of the replication flow to delete" };

    private readonly Option<bool> _yesOption = new("--yes", "-y") { Description = "Skip confirmation prompt" };

    public DeleteMirrorCommand() : base("delete", "Delete a replication flow")
    {
        Arguments.Add(_nameArgument);
        Options.Add(_yesOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArgument);
        var localYes = parseResult.GetValue(_yesOption);
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));

        try
        {
            if (!ConfirmDestructive(parseResult, $"Are you sure you want to delete replication flow '[bold]{name}[/]'?", localYes))
            {
                WriteWarning("Delete cancelled.");
                return 0;
            }

            WriteVerbose(parseResult, $"Deleting replication flow '{name}'...");

            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Delete all mirror connectors (source, checkpoint, heartbeat)
            var connectorNames = new[] { $"{name}-source", $"{name}-checkpoint", $"{name}-heartbeat" };
            var deletedCount = 0;

            foreach (var connectorName in connectorNames)
            {
                try
                {
                    await client.Connect.DeleteConnectorAsync(connectorName, ct);
                    WriteVerbose(parseResult, $"  Deleted connector '{connectorName}'");
                    deletedCount++;
                }
                catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase))
                {
                    // Connector doesn't exist (e.g., heartbeat disabled), skip it
                    WriteVerbose(parseResult, $"  Connector '{connectorName}' not found, skipping");
                }
            }

            if (deletedCount == 0)
            {
                WriteError($"No connectors found for replication flow '{name}'");
                return 1;
            }

            WriteSuccess($"Replication flow '{name}' deleted successfully ({deletedCount} connector(s)).");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete replication flow: {ex.Message}");
            return 1;
        }
    }
}
