using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Dlq;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

/// <summary>
/// Show DLQ messages with error details (surgewave dlq describe)
/// </summary>
public class DescribeDlqCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("dlq-topic")
    {
        Description = "Name of the DLQ topic to describe"
    };

    private readonly Option<int> _maxMessagesOpt = new("--max-messages", "-n")
    {
        Description = "Maximum messages to show",
        DefaultValueFactory = _ => 10
    };

    private readonly Option<int> _partitionOpt = new("--partition", "-p")
    {
        Description = "Partition to read from (-1 for all)",
        DefaultValueFactory = _ => -1
    };

    private readonly Option<string> _offsetOpt = new("--offset", "-o")
    {
        Description = "Starting offset (earliest, latest, or number)",
        DefaultValueFactory = _ => "earliest"
    };

    private readonly Option<bool> _showStackTraceOpt = new("--stacktrace", "-t")
    {
        Description = "Show full stack traces",
        DefaultValueFactory = _ => false
    };

    public DescribeDlqCommand() : base("describe", "Show DLQ messages with error details")
    {
        Arguments.Add(_topicArg);
        Options.Add(_maxMessagesOpt);
        Options.Add(_partitionOpt);
        Options.Add(_offsetOpt);
        Options.Add(_showStackTraceOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var topic = parseResult.GetValue(_topicArg);
        var maxMessages = parseResult.GetValue(_maxMessagesOpt);
        var partition = parseResult.GetValue(_partitionOpt);
        var offsetStr = parseResult.GetValue(_offsetOpt);
        var showStackTrace = parseResult.GetValue(_showStackTraceOpt);

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Check if topic exists
            var topics = await client.Topics.ListAsync(ct);
            var topicInfo = topics.FirstOrDefault(t => t.Name == topic);
            if (topicInfo == null)
            {
                WriteError($"DLQ topic '{topic}' not found");
                return 1;
            }

            var records = new List<DlqRecord>();
            var partitions = partition >= 0
                ? [partition]
                : Enumerable.Range(0, topicInfo.PartitionCount).ToList();

            foreach (var p in partitions)
            {
                if (records.Count >= maxMessages) break;

                long startOffset;
                if (offsetStr?.ToLowerInvariant() == "earliest")
                {
                    startOffset = await client.Messaging.GetEarliestOffsetAsync(topic!, p, ct);
                }
                else if (offsetStr?.ToLowerInvariant() == "latest")
                {
                    var latest = await client.Messaging.GetLatestOffsetAsync(topic!, p, ct);
                    startOffset = Math.Max(0, latest - maxMessages);
                }
                else if (long.TryParse(offsetStr, out var parsed))
                {
                    startOffset = parsed;
                }
                else
                {
                    startOffset = await client.Messaging.GetEarliestOffsetAsync(topic!, p, ct);
                }

                var currentOffset = startOffset;
                var remaining = maxMessages - records.Count;

                while (remaining > 0 && !ct.IsCancellationRequested)
                {
                    var result = await client.Messaging.ReceiveAsync(topic!, p, currentOffset, cancellationToken: ct);
                    if (result.Messages.Count == 0) break;

                    foreach (var msg in result.Messages)
                    {
                        currentOffset = msg.Offset + 1;

                        // Try to deserialize as DLQ record
                        var record = DlqRecordSerializer.Deserialize(msg.Value ?? []);
                        if (record != null)
                        {
                            records.Add(record);
                            remaining--;
                            if (remaining <= 0) break;
                        }
                    }
                }
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(records, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var record in records)
                {
                    Console.WriteLine($"{record.OriginalTopic}\t{record.OriginalPartition}\t{record.OriginalOffset}\t{record.ExceptionType}\t{record.ExceptionMessage}");
                }
            }
            else
            {
                if (records.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No DLQ records found.[/]");
                    return 0;
                }

                AnsiConsole.MarkupLine($"[bold]DLQ Topic:[/] {topic}");
                AnsiConsole.MarkupLine($"[bold]Records:[/] {records.Count}");
                AnsiConsole.WriteLine();

                foreach (var record in records)
                {
                    var panel = new Panel(BuildRecordContent(record, showStackTrace))
                    {
                        Header = new PanelHeader($"[red]{record.ExceptionType}[/]"),
                        Border = BoxBorder.Rounded
                    };
                    AnsiConsole.Write(panel);
                    AnsiConsole.WriteLine();
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe DLQ: {ex.Message}");
            return 1;
        }
    }

    private static string BuildRecordContent(DlqRecord record, bool showStackTrace)
    {
        var sb = new StringBuilder();

        sb.AppendLine($"[bold]Original:[/] {record.OriginalTopic}[{record.OriginalPartition}] @ {record.OriginalOffset}");
        sb.AppendLine($"[bold]Source:[/] {record.SourceType} ({record.SourceName})");

        if (!string.IsNullOrEmpty(record.TaskId))
        {
            sb.AppendLine($"[bold]Task ID:[/] {record.TaskId}");
        }

        sb.AppendLine($"[bold]Attempt:[/] {record.AttemptCount}");
        sb.AppendLine($"[bold]Failed At:[/] {record.FailedAt:O}");
        sb.AppendLine();
        sb.AppendLine($"[red]{record.ExceptionMessage}[/]");

        if (showStackTrace && !string.IsNullOrEmpty(record.StackTrace))
        {
            sb.AppendLine();
            sb.AppendLine("[dim]Stack Trace:[/]");
            sb.AppendLine($"[dim]{record.StackTrace}[/]");
        }

        if (record.OriginalValue != null && record.OriginalValue.Length > 0)
        {
            sb.AppendLine();
            var valuePreview = Encoding.UTF8.GetString(record.OriginalValue);
            if (valuePreview.Length > 200)
            {
                valuePreview = valuePreview[..200] + "...";
            }
            sb.AppendLine($"[bold]Value Preview:[/] {valuePreview}");
        }

        return sb.ToString();
    }
}
