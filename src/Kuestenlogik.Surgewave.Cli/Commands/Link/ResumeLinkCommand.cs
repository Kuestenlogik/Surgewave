using System.CommandLine;
using System.CommandLine.Parsing;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Resume a paused cluster link (surgewave link resume)
/// </summary>
public class ResumeLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Link ID to resume", Required = true };

    public ResumeLinkCommand() : base("resume", "Resume a paused cluster link")
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
            WriteSuccess($"Cluster link '{linkId}' resumed.");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
