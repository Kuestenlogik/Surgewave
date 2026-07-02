using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Cli.Commands.Link;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Promote a mirror topic to writable (planned migration) (surgewave mirror promote)
/// </summary>
public class PromoteMirrorCommand : CommandBase
{
    private readonly Argument<string> _topicArgument = new("topic") { Description = "Name of the mirror topic to promote" };
    private readonly Option<string> _linkOption = new("--link", "-l") { Description = "Cluster link ID the mirror topic belongs to", Required = true };

    public PromoteMirrorCommand() : base("promote", "Promote a mirror topic to writable (planned migration)")
    {
        Arguments.Add(_topicArgument);
        Options.Add(_linkOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArgument)!;
        var linkId = parseResult.GetValue(_linkOption)!;
        var format = GetFormat(parseResult);

        try
        {
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.PostAsync(
                $"/api/cluster-links/{Uri.EscapeDataString(linkId)}/mirror-topics/{Uri.EscapeDataString(topic)}/promote",
                null,
                ct);
            var body = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to promote mirror topic '{topic}': {response.StatusCode} — {LinkApi.ExtractErrorMessage(body)}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(body);
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"promoted {topic} link={linkId}");
            }
            else
            {
                WriteSuccess($"Mirror topic '{topic}' promoted to writable.");
                WriteWarning("Consumers may need to restart to pick up the new state.");
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
