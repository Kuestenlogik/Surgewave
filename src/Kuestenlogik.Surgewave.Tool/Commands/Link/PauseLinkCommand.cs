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
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.PostAsync($"/api/cluster-links/{Uri.EscapeDataString(linkId)}/pause", null, ct);

            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                WriteError($"Failed to pause cluster link '{linkId}': {response.StatusCode} — {LinkApi.ExtractErrorMessage(body)}");
                return 1;
            }

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
