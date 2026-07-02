using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Describe a cluster link (surgewave link describe)
/// </summary>
public class DescribeLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Link ID to describe", Required = true };

    public DescribeLinkCommand() : base("describe", "Show details of a cluster link")
    {
        Aliases.Add("show");
        Options.Add(_linkIdOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var linkId = parseResult.GetValue(_linkIdOption)!;
        var format = GetFormat(parseResult);

        try
        {
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.GetAsync($"/api/cluster-links/{Uri.EscapeDataString(linkId)}", ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to describe cluster link '{linkId}': {response.StatusCode} — {LinkApi.ExtractErrorMessage(json)}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
            }
            else if (format == OutputFormat.Plain)
            {
                using var doc = JsonDocument.Parse(json);
                var status = doc.RootElement;
                Console.WriteLine(
                    $"{LinkApi.GetString(status, "linkId")}\t{LinkApi.GetString(status, "state")}\t" +
                    $"{LinkApi.GetString(status, "remoteClusterId")}\t{LinkApi.GetInt64(status, "mirroredTopicCount")}\t" +
                    $"{LinkApi.GetInt64(status, "totalLag")}\t{LinkApi.GetString(status, "lastFetch")}");
            }
            else
            {
                using var doc = JsonDocument.Parse(json);
                LinkApi.WriteStatusGrid(doc.RootElement);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
