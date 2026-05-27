using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Dlq;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

/// <summary>
/// Replay messages from DLQ back to original topic (surgewave dlq replay)
/// </summary>
public class ReplayDlqCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("dlq-topic")
    {
        Description = "Name of the DLQ topic to replay from"
    };

    private readonly Option<string?> _targetTopicOpt = new("--target-topic", "-t")
    {
        Description = "Target topic to replay to (defaults to original topic)"
    };

    private readonly Option<int> _maxMessagesOpt = new("--max-messages", "-n")
    {
        Description = "Maximum messages to replay (-1 for all)",
        DefaultValueFactory = _ => -1
    };

    private readonly Option<int> _partitionOpt = new("--partition", "-p")
    {
        Description = "Source partition to replay from (-1 for all)",
        DefaultValueFactory = _ => -1
    };

    private readonly Option<bool> _dryRunOpt = new("--dry-run")
    {
        Description = "Show what would be replayed without actually replaying",
        DefaultValueFactory = _ => false
    };

    private readonly Option<bool> _preservePartitionOpt = new("--preserve-partition")
    {
        Description = "Replay to original partition (if possible)",
        DefaultValueFactory = _ => false
    };

    public ReplayDlqCommand() : base("replay", "Replay messages from DLQ back to original topic")
    {
        Arguments.Add(_topicArg);
        Options.Add(_targetTopicOpt);
        Options.Add(_maxMessagesOpt);
        Options.Add(_partitionOpt);
        Options.Add(_dryRunOpt);
        Options.Add(_preservePartitionOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var dlqTopic = parseResult.GetValue(_topicArg);
        var targetTopic = parseResult.GetValue(_targetTopicOpt);
        var maxMessages = parseResult.GetValue(_maxMessagesOpt);
        var partition = parseResult.GetValue(_partitionOpt);
        var dryRun = parseResult.GetValue(_dryRunOpt);
        var preservePartition = parseResult.GetValue(_preservePartitionOpt);

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Check if DLQ topic exists
            var topics = await client.Topics.ListAsync(ct);
            var topicInfo = topics.FirstOrDefault(t => t.Name == dlqTopic);
            if (topicInfo == null)
            {
                WriteError($"DLQ topic '{dlqTopic}' not found");
                return 1;
            }

            var partitions = partition >= 0
                ? [partition]
                : Enumerable.Range(0, topicInfo.PartitionCount).ToList();

            var replayed = 0;
            var failed = 0;
            var replayResults = new List<ReplayResult>();

            foreach (var p in partitions)
            {
                var startOffset = await client.Messaging.GetEarliestOffsetAsync(dlqTopic!, p, ct);
                var endOffset = await client.Messaging.GetLatestOffsetAsync(dlqTopic!, p, ct);
                var currentOffset = startOffset;

                while (currentOffset < endOffset && !ct.IsCancellationRequested)
                {
                    if (maxMessages >= 0 && replayed >= maxMessages) break;

                    var result = await client.Messaging.ReceiveAsync(dlqTopic!, p, currentOffset, cancellationToken: ct);
                    if (result.Messages.Count == 0) break;

                    foreach (var msg in result.Messages)
                    {
                        currentOffset = msg.Offset + 1;

                        var record = DlqRecordSerializer.Deserialize(msg.Value ?? []);
                        if (record == null)
                        {
                            failed++;
                            continue;
                        }

                        var replayTarget = targetTopic ?? record.OriginalTopic;
                        var replayPartition = preservePartition ? record.OriginalPartition : 0;

                        if (dryRun)
                        {
                            replayResults.Add(new ReplayResult
                            {
                                OriginalTopic = record.OriginalTopic,
                                OriginalPartition = record.OriginalPartition,
                                OriginalOffset = record.OriginalOffset,
                                TargetTopic = replayTarget,
                                TargetPartition = replayPartition,
                                ExceptionType = record.ExceptionType
                            });
                            replayed++;
                        }
                        else
                        {
                            try
                            {
                                var keyString = record.OriginalKey != null
                                    ? System.Text.Encoding.UTF8.GetString(record.OriginalKey)
                                    : null;
                                var valueString = record.OriginalValue != null
                                    ? System.Text.Encoding.UTF8.GetString(record.OriginalValue)
                                    : "";

                                await client.Messaging.SendAsync(
                                    replayTarget,
                                    replayPartition,
                                    keyString,
                                    valueString,
                                    ct);

                                replayResults.Add(new ReplayResult
                                {
                                    OriginalTopic = record.OriginalTopic,
                                    OriginalPartition = record.OriginalPartition,
                                    OriginalOffset = record.OriginalOffset,
                                    TargetTopic = replayTarget,
                                    TargetPartition = replayPartition,
                                    ExceptionType = record.ExceptionType
                                });
                                replayed++;
                            }
                            catch (Exception ex)
                            {
                                WriteError($"Failed to replay message from offset {msg.Offset}: {ex.Message}");
                                failed++;
                            }
                        }

                        if (maxMessages >= 0 && replayed >= maxMessages) break;
                    }
                }
            }

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    DryRun = dryRun,
                    Replayed = replayed,
                    Failed = failed,
                    Results = replayResults
                };
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var r in replayResults)
                {
                    Console.WriteLine($"{r.OriginalTopic}\t{r.OriginalPartition}\t{r.OriginalOffset}\t{r.TargetTopic}\t{r.TargetPartition}");
                }
                Console.WriteLine($"Replayed: {replayed}, Failed: {failed}");
            }
            else
            {
                if (dryRun)
                {
                    AnsiConsole.MarkupLine("[yellow]DRY RUN - No messages were actually replayed[/]");
                    AnsiConsole.WriteLine();
                }

                if (replayResults.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No messages to replay.[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Original Topic");
                table.AddColumn("Original Offset");
                table.AddColumn("Target Topic");
                table.AddColumn("Error Type");

                foreach (var r in replayResults.Take(50)) // Limit display
                {
                    table.AddRow(
                        $"[cyan]{r.OriginalTopic}[/]",
                        $"{r.OriginalPartition}:{r.OriginalOffset}",
                        $"[green]{r.TargetTopic}[/]",
                        $"[red]{r.ExceptionType}[/]"
                    );
                }

                AnsiConsole.Write(table);

                if (replayResults.Count > 50)
                {
                    AnsiConsole.MarkupLine($"[dim]... and {replayResults.Count - 50} more[/]");
                }

                AnsiConsole.WriteLine();
                var verb = dryRun ? "Would replay" : "Replayed";
                WriteSuccess($"{verb} {replayed} message(s), {failed} failed");
            }

            return failed > 0 ? 1 : 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to replay DLQ: {ex.Message}");
            return 1;
        }
    }

    private sealed class ReplayResult
    {
        public string OriginalTopic { get; init; } = "";
        public int OriginalPartition { get; init; }
        public long OriginalOffset { get; init; }
        public string TargetTopic { get; init; } = "";
        public int TargetPartition { get; init; }
        public string ExceptionType { get; init; } = "";
    }
}
