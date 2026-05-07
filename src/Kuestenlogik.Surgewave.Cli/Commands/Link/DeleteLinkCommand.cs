using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Delete a cluster link (surgewave link delete)
/// </summary>
public class DeleteLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Link ID to delete", Required = true };
    private readonly Option<bool> _forceOption = new("--force") { Description = "Force deletion without confirmation" };

    public DeleteLinkCommand() : base("delete", "Delete a cluster link")
    {
        Options.Add(_linkIdOption);
        Options.Add(_forceOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var linkId = parseResult.GetValue(_linkIdOption)!;
        var force = parseResult.GetValue(_forceOption);

        if (!force)
        {
            var confirm = AnsiConsole.Confirm($"Delete cluster link '{linkId}'? This will stop all mirror topics.", false);
            if (!confirm)
            {
                WriteWarning("Cancelled.");
                return 1;
            }
        }

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Placeholder - will be wired to actual API
            WriteSuccess($"Cluster link '{linkId}' deleted.");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
