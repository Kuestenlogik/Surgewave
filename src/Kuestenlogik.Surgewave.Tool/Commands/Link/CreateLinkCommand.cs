using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Create a cluster link for geo-replication (surgewave link create)
/// </summary>
public class CreateLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Unique identifier for this cluster link", Required = true };
    private readonly Option<string> _remoteOption = new("--remote", "-r") { Description = "Remote bootstrap servers (host:port,...)", Required = true };
    private readonly Option<string> _topicFilterOption = new("--topic-filter") { Description = "Regex filter for topics to replicate", DefaultValueFactory = _ => ".*" };
    private readonly Option<int> _fetcherThreadsOption = new("--fetcher-threads") { Description = "Number of fetcher threads", DefaultValueFactory = _ => 4 };
    private readonly Option<int> _fetchIntervalOption = new("--fetch-interval-ms") { Description = "Fetch interval in milliseconds", DefaultValueFactory = _ => 500 };
    private readonly Option<bool> _syncOffsetsOption = new("--sync-offsets") { Description = "Sync consumer group offsets", DefaultValueFactory = _ => true };
    private readonly Option<bool> _syncConfigsOption = new("--sync-configs") { Description = "Sync topic configurations", DefaultValueFactory = _ => true };

    public CreateLinkCommand() : base("create", "Create a new cluster link")
    {
        Options.Add(_linkIdOption);
        Options.Add(_remoteOption);
        Options.Add(_topicFilterOption);
        Options.Add(_fetcherThreadsOption);
        Options.Add(_fetchIntervalOption);
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
        var fetcherThreads = parseResult.GetValue(_fetcherThreadsOption);
        var fetchIntervalMs = parseResult.GetValue(_fetchIntervalOption);
        var syncOffsets = parseResult.GetValue(_syncOffsetsOption);
        var syncConfigs = parseResult.GetValue(_syncConfigsOption);
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Creating cluster link '{linkId}' to {remote}...");

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var config = new
            {
                LinkId = linkId,
                RemoteBootstrapServers = remote,
                TopicFilter = topicFilter,
                FetcherThreads = fetcherThreads,
                FetchIntervalMs = fetchIntervalMs,
                SyncConsumerOffsets = syncOffsets,
                SyncTopicConfigs = syncConfigs
            };

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(config, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"created {linkId} remote={remote}");
            }
            else
            {
                var table = new Table();
                table.AddColumn("Property");
                table.AddColumn("Value");
                table.Title = new TableTitle($"[bold]Cluster Link: {linkId}[/]");

                table.AddRow("Link ID", linkId);
                table.AddRow("Remote", remote);
                table.AddRow("Topic Filter", topicFilter ?? ".*");
                table.AddRow("Fetcher Threads", fetcherThreads.ToString());
                table.AddRow("Fetch Interval", $"{fetchIntervalMs}ms");
                table.AddRow("Sync Offsets", syncOffsets.ToString());
                table.AddRow("Sync Configs", syncConfigs.ToString());

                AnsiConsole.Write(table);
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
