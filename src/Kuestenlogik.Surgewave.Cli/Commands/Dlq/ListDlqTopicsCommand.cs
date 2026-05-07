using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

/// <summary>
/// List all DLQ topics with message counts (surgewave dlq list)
/// </summary>
public class ListDlqTopicsCommand : CommandBase
{
    private readonly Option<string> _suffixOpt = new("--suffix", "-s")
    {
        Description = "DLQ topic suffix to filter by",
        DefaultValueFactory = _ => ".DLQ"
    };

    public ListDlqTopicsCommand() : base("list", "List all DLQ topics")
    {
        Options.Add(_suffixOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var suffix = parseResult.GetValue(_suffixOpt) ?? ".DLQ";

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var allTopics = await client.Topics.ListAsync(ct);
            var dlqTopics = allTopics
                .Where(t => t.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                .OrderBy(t => t.Name)
                .ToList();

            // Get message counts for each DLQ topic
            var dlqInfo = new List<DlqTopicInfo>();
            foreach (var topic in dlqTopics)
            {
                long totalMessages = 0;
                for (int p = 0; p < topic.PartitionCount; p++)
                {
                    var earliest = await client.Messaging.GetEarliestOffsetAsync(topic.Name, p, ct);
                    var latest = await client.Messaging.GetLatestOffsetAsync(topic.Name, p, ct);
                    totalMessages += latest - earliest;
                }

                var originalTopic = topic.Name[..^suffix.Length];
                dlqInfo.Add(new DlqTopicInfo
                {
                    DlqTopic = topic.Name,
                    OriginalTopic = originalTopic,
                    PartitionCount = topic.PartitionCount,
                    MessageCount = totalMessages
                });
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(dlqInfo, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var info in dlqInfo)
                {
                    Console.WriteLine($"{info.DlqTopic}\t{info.OriginalTopic}\t{info.MessageCount}");
                }
            }
            else
            {
                if (dlqInfo.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No DLQ topics found.[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("DLQ Topic");
                table.AddColumn("Original Topic");
                table.AddColumn("Partitions");
                table.AddColumn("Messages");

                foreach (var info in dlqInfo)
                {
                    var messageColor = info.MessageCount > 0 ? "yellow" : "dim";
                    table.AddRow(
                        $"[red]{info.DlqTopic}[/]",
                        $"[cyan]{info.OriginalTopic}[/]",
                        info.PartitionCount.ToString(),
                        $"[{messageColor}]{info.MessageCount}[/]"
                    );
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]{dlqInfo.Count} DLQ topic(s)[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list DLQ topics: {ex.Message}");
            return 1;
        }
    }

    private sealed class DlqTopicInfo
    {
        public string DlqTopic { get; init; } = "";
        public string OriginalTopic { get; init; } = "";
        public int PartitionCount { get; init; }
        public long MessageCount { get; init; }
    }
}
