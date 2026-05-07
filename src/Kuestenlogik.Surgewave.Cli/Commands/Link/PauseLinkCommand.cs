using System.CommandLine;
using System.CommandLine.Parsing;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Pause a cluster link (surgewave link pause)
/// </summary>
public class PauseLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Link ID to pause", Required = true };

    public PauseLinkCommand() : base("pause", "Pause a cluster link")
    {
        Options.Add(_linkIdOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var linkId = parseResult.GetValue(_linkIdOption)!;

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Placeholder - will be wired to actual API
            WriteSuccess($"Cluster link '{linkId}' paused.");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
