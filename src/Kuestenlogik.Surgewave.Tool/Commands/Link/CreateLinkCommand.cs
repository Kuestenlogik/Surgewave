using System.CommandLine;
using System.CommandLine.Parsing;
using System.Net.Http.Json;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Create a cluster link for geo-replication (surgewave link create)
/// </summary>
public class CreateLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Unique identifier for this cluster link", Required = true };
    private readonly Option<string> _remoteOption = new("--remote", "-r") { Description = "Remote bootstrap servers (host:port,...)", Required = true };
    private readonly Option<string?> _topicFilterOption = new("--topic-filter") { Description = "Regex filter for topics to replicate (default: all topics)" };
    private readonly Option<bool> _syncOffsetsOption = new("--sync-offsets") { Description = "Sync consumer group offsets", DefaultValueFactory = _ => true };
    private readonly Option<bool> _syncConfigsOption = new("--sync-configs") { Description = "Sync topic configurations", DefaultValueFactory = _ => true };

    public CreateLinkCommand() : base("create", "Create a new cluster link")
    {
        Options.Add(_linkIdOption);
        Options.Add(_remoteOption);
        Options.Add(_topicFilterOption);
        Options.Add(_syncOffsetsOption);
        Options.Add(_syncConfigsOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var linkId = parseResult.GetValue(_linkIdOption)!;
        var remote = parseResult.GetValue(_remoteOption)!;
        var topicFilter = parseResult.GetValue(_topicFilterOption);
        var syncOffsets = parseResult.GetValue(_syncOffsetsOption);
        var syncConfigs = parseResult.GetValue(_syncConfigsOption);
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Creating cluster link '{linkId}' to {remote}...");

        try
        {
            using var http = BrokerAdminHttp.Create(host);

            var request = new
            {
                linkId,
                remoteBootstrapServers = remote,
                topicFilter,
                syncConsumerOffsets = syncOffsets,
                syncTopicConfigs = syncConfigs
            };

            var response = await http.PostAsJsonAsync("/api/cluster-links", request, ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to create cluster link: {response.StatusCode} — {LinkApi.ExtractErrorMessage(json)}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"created {linkId} remote={remote}");
            }
            else
            {
                using var doc = JsonDocument.Parse(json);
                LinkApi.WriteStatusGrid(doc.RootElement);
                WriteSuccess($"Cluster link '{linkId}' created.");
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
