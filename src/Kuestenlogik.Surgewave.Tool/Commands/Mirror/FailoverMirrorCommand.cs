using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Cli.Commands.Link;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Fail over a mirror topic to writable (unplanned, source unreachable) (surgewave mirror failover)
/// </summary>
public class FailoverMirrorCommand : CommandBase
{
    private readonly Argument<string> _topicArgument = new("topic") { Description = "Name of the mirror topic to fail over" };
    private readonly Option<string> _linkOption = new("--link", "-l") { Description = "Cluster link ID the mirror topic belongs to", Required = true };
    private readonly Option<bool> _yesOption = new("--yes", "-y") { Description = "Skip confirmation prompt" };

    public FailoverMirrorCommand() : base("failover", "Fail over a mirror topic to writable (unplanned migration)")
    {
        Arguments.Add(_topicArgument);
        Options.Add(_linkOption);
        Options.Add(_yesOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArgument)!;
        var linkId = parseResult.GetValue(_linkOption)!;
        var localYes = parseResult.GetValue(_yesOption);
        var format = GetFormat(parseResult);

        if (!ConfirmDestructive(
                parseResult,
                $"Fail over mirror topic '[bold]{topic}[/]' on link '{linkId}'? Records not yet replicated will be lost.",
                localYes))
        {
            WriteWarning("Failover cancelled.");
            return 0;
        }

        try
        {
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.PostAsync(
                $"/api/cluster-links/{Uri.EscapeDataString(linkId)}/mirror-topics/{Uri.EscapeDataString(topic)}/failover",
                null,
                ct);
            var body = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to fail over mirror topic '{topic}': {response.StatusCode} — {LinkApi.ExtractErrorMessage(body)}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(body);
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"failed-over {topic} link={linkId}");
            }
            else
            {
                WriteSuccess($"Mirror topic '{topic}' failed over to writable.");
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
