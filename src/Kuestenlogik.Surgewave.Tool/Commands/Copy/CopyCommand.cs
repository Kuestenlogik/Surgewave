using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Copy;

/// <summary>
/// Command for copying messages between topics (surgewave copy)
/// Efficiently transfers messages from source to destination topic.
/// Supports filtering, transformations, and progress reporting.
/// </summary>
public class CopyCommand : CommandBase
{
    private readonly Argument<string> _sourceTopicArg = new("source") { Description = "Source topic to copy from" };
    private readonly Argument<string> _destTopicArg = new("destination") { Description = "Destination topic to copy to" };
    private readonly Option<string> _offsetOpt = new("--offset", "-o") { Description = "Starting offset (earliest, latest, or number)", DefaultValueFactory = _ => "earliest" };
    private readonly Option<int> _sourcePartitionOpt = new("--source-partition", "-sp") { Description = "Source partition to read from", DefaultValueFactory = _ => 0 };
    private readonly Option<int> _destPartitionOpt = new("--dest-partition", "-dp") { Description = "Destination partition to write to", DefaultValueFactory = _ => 0 };
    private readonly Option<int> _maxMessagesOpt = new("--max-messages", "-n") { Description = "Maximum messages to copy (-1 for all)", DefaultValueFactory = _ => -1 };
    private readonly Option<bool> _preserveKeysOpt = new("--preserve-keys", "-k") { Description = "Preserve message keys", DefaultValueFactory = _ => true };
    private readonly Option<string?> _keyOpt = new("--key") { Description = "Override all message keys with this value" };
    private readonly Option<bool> _dryRunOpt = new("--dry-run") { Description = "Show what would be copied without actually copying", DefaultValueFactory = _ => false };
    private readonly Option<int> _batchSizeOpt = new("--batch-size") { Description = "Number of messages to fetch per batch", DefaultValueFactory = _ => 100 };

    public CopyCommand() : base("copy", "Copy messages from one topic to another")
    {
        Arguments.Add(_sourceTopicArg);
        Arguments.Add(_destTopicArg);
        Options.Add(_offsetOpt);
        Options.Add(_sourcePartitionOpt);
        Options.Add(_destPartitionOpt);
        Options.Add(_maxMessagesOpt);
        Options.Add(_preserveKeysOpt);
        Options.Add(_keyOpt);
        Options.Add(_dryRunOpt);
        Options.Add(_batchSizeOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var sourceTopic = parseResult.GetValue(_sourceTopicArg);
        var destTopic = parseResult.GetValue(_destTopicArg);
        var offsetStr = parseResult.GetValue(_offsetOpt);
        var sourcePartition = parseResult.GetValue(_sourcePartitionOpt);
        var destPartition = parseResult.GetValue(_destPartitionOpt);
        var maxMessages = parseResult.GetValue(_maxMessagesOpt);
        var preserveKeys = parseResult.GetValue(_preserveKeysOpt);
        var keyOverride = parseResult.GetValue(_keyOpt);
        var dryRun = parseResult.GetValue(_dryRunOpt);
        var batchSize = parseResult.GetValue(_batchSizeOpt);
        var format = GetFormat(parseResult);

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync(ct);

        // Determine starting offset
        long startOffset = await ResolveStartOffsetAsync(client, sourceTopic!, sourcePartition, offsetStr, ct);

        // Get end offset to know how many messages exist
        var endOffset = await client.Messaging.GetLatestOffsetAsync(sourceTopic!, sourcePartition, ct);
        var availableMessages = endOffset - startOffset;

        if (availableMessages <= 0)
        {
            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Status = "no_messages", SourceTopic = sourceTopic, DestTopic = destTopic }));
            }
            else
            {
                AnsiConsole.MarkupLine($"[yellow]No messages to copy from '{sourceTopic}'[{sourcePartition}][/]");
            }
            return 0;
        }

        var totalToCopy = maxMessages > 0 ? Math.Min(maxMessages, availableMessages) : availableMessages;

        if (dryRun)
        {
            OutputDryRun(format, sourceTopic!, destTopic!, sourcePartition, destPartition, startOffset, totalToCopy);
            return 0;
        }

        WriteVerbose(parseResult, $"Copying from {sourceTopic}[{sourcePartition}] to {destTopic}[{destPartition}]...");

        var copied = 0L;
        var currentOffset = startOffset;
        var startTime = DateTime.UtcNow;

        // Progress display for table format
        if (format == OutputFormat.Table)
        {
            await AnsiConsole.Progress()
                .AutoClear(false)
                .HideCompleted(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new RemainingTimeColumn(),
                    new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask($"[cyan]Copying to {destTopic}[/]", maxValue: totalToCopy);

                    while (!ct.IsCancellationRequested && copied < totalToCopy)
                    {
                        var result = await client.Messaging.ReceiveAsync(sourceTopic!, sourcePartition, currentOffset, batchSize, maxWaitMs: 0, ct);

                        if (result.Messages.Count == 0)
                        {
                            break;
                        }

                        foreach (var msg in result.Messages)
                        {
                            if (copied >= totalToCopy) break;

                            var key = keyOverride ?? (preserveKeys ? msg.KeyString : null);
                            await client.Messaging.SendAsync(destTopic!, destPartition, key, msg.ValueString, ct);

                            currentOffset = msg.Offset + 1;
                            copied++;
                            task.Increment(1);
                        }
                    }

                    task.StopTask();
                });
        }
        else
        {
            // Plain/JSON format - simpler output
            while (!ct.IsCancellationRequested && copied < totalToCopy)
            {
                var result = await client.Messaging.ReceiveAsync(sourceTopic!, sourcePartition, currentOffset, batchSize, maxWaitMs: 0, ct);

                if (result.Messages.Count == 0)
                {
                    break;
                }

                foreach (var msg in result.Messages)
                {
                    if (copied >= totalToCopy) break;

                    var key = keyOverride ?? (preserveKeys ? msg.KeyString : null);
                    await client.Messaging.SendAsync(destTopic!, destPartition, key, msg.ValueString, ct);

                    currentOffset = msg.Offset + 1;
                    copied++;

                    if (format == OutputFormat.Plain && copied % 10000 == 0)
                    {
                        Console.WriteLineToError($"Copied {copied:N0}/{totalToCopy:N0} messages...");
                    }
                }
            }
        }

        var elapsed = DateTime.UtcNow - startTime;
        OutputSummary(format, sourceTopic!, destTopic!, sourcePartition, destPartition, copied, elapsed);
        return 0;
    }

    private async Task<long> ResolveStartOffsetAsync(
        SurgewaveNativeClient client,
        string topic,
        int partition,
        string? offsetStr,
        CancellationToken ct)
    {
        if (offsetStr?.ToLowerInvariant() == "earliest")
        {
            return await client.Messaging.GetEarliestOffsetAsync(topic, partition, ct);
        }
        else if (offsetStr?.ToLowerInvariant() == "latest")
        {
            return await client.Messaging.GetLatestOffsetAsync(topic, partition, ct);
        }
        else if (long.TryParse(offsetStr, out var parsed))
        {
            return parsed;
        }
        else
        {
            return await client.Messaging.GetEarliestOffsetAsync(topic, partition, ct);
        }
    }

    private static void OutputDryRun(
        OutputFormat format,
        string sourceTopic,
        string destTopic,
        int sourcePartition,
        int destPartition,
        long startOffset,
        long totalToCopy)
    {
        if (format == OutputFormat.Json)
        {
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                DryRun = true,
                SourceTopic = sourceTopic,
                SourcePartition = sourcePartition,
                DestTopic = destTopic,
                DestPartition = destPartition,
                StartOffset = startOffset,
                MessageCount = totalToCopy
            }));
        }
        else
        {
            AnsiConsole.MarkupLine("[yellow]DRY RUN - No messages will be copied[/]");
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"Source: [cyan]{sourceTopic}[/][{sourcePartition}] @ offset {startOffset}");
            AnsiConsole.MarkupLine($"Destination: [green]{destTopic}[/][{destPartition}]");
            AnsiConsole.MarkupLine($"Messages to copy: [bold]{totalToCopy:N0}[/]");
        }
    }

    private static void OutputSummary(
        OutputFormat format,
        string sourceTopic,
        string destTopic,
        int sourcePartition,
        int destPartition,
        long copied,
        TimeSpan elapsed)
    {
        var rate = elapsed.TotalSeconds > 0 ? copied / elapsed.TotalSeconds : copied;

        if (format == OutputFormat.Json)
        {
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Status = "completed",
                SourceTopic = sourceTopic,
                SourcePartition = sourcePartition,
                DestTopic = destTopic,
                DestPartition = destPartition,
                MessagesCopied = copied,
                ElapsedMs = (long)elapsed.TotalMilliseconds,
                MessagesPerSecond = rate
            }));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"Copied {copied:N0} messages from {sourceTopic}[{sourcePartition}] to {destTopic}[{destPartition}] in {elapsed.TotalSeconds:F1}s ({rate:N0} msg/s)");
        }
        else
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[green]Copied {copied:N0} messages[/] from [cyan]{sourceTopic}[/][{sourcePartition}] to [cyan]{destTopic}[/][{destPartition}]");
            AnsiConsole.MarkupLine($"[dim]Elapsed: {elapsed.TotalSeconds:F1}s ({rate:N0} msg/s)[/]");
        }
    }
}
