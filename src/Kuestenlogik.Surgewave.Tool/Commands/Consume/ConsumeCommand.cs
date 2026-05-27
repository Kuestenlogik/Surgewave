using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Consume;

/// <summary>
/// Command for consuming messages (surgewave consume)
/// Supports piping: surgewave consume topic | grep "pattern"
/// Auto-detects piped output and switches to plain format
/// </summary>
public class ConsumeCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Topic to consume from" };
    private readonly Option<string> _offsetOpt = new("--offset", "-o") { Description = "Starting offset (earliest, latest, or number)", DefaultValueFactory = _ => "latest" };
    private readonly Option<int> _partitionOpt = new("--partition", "-p") { Description = "Partition to consume from", DefaultValueFactory = _ => 0 };
    private readonly Option<int> _maxMessagesOpt = new("--max-messages", "-n") { Description = "Maximum messages to consume (-1 for unlimited)", DefaultValueFactory = _ => -1 };
    private readonly Option<bool> _showKeysOpt = new("--keys", "-k") { Description = "Show message keys", DefaultValueFactory = _ => true };
    private readonly Option<bool> _showTimestampsOpt = new("--timestamps", "-t") { Description = "Show message timestamps", DefaultValueFactory = _ => false };
    private readonly Option<string> _separatorOpt = new("--separator", "-s") { Description = "Key-value separator for plain output", DefaultValueFactory = _ => ":" };
    private readonly Option<bool> _printOffsetOpt = new("--print-offset") { Description = "Print offset before each message (plain mode)", DefaultValueFactory = _ => false };
    private readonly Option<string?> _outputOpt = new("--output") { Description = "Output file path (writes consumed messages to file)" };
    private readonly Option<string> _outputFormatOpt = new("--output-format") { Description = "Output format: string, json, jsonl, binary", DefaultValueFactory = _ => "string" };

    public ConsumeCommand() : base("consume", "Consume messages from a topic")
    {
        Arguments.Add(_topicArg);
        Options.Add(_offsetOpt);
        Options.Add(_partitionOpt);
        Options.Add(_maxMessagesOpt);
        Options.Add(_showKeysOpt);
        Options.Add(_showTimestampsOpt);
        Options.Add(_separatorOpt);
        Options.Add(_printOffsetOpt);
        Options.Add(_outputOpt);
        Options.Add(_outputFormatOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var offsetStr = parseResult.GetValue(_offsetOpt);
        var partition = parseResult.GetValue(_partitionOpt);
        var maxMessages = parseResult.GetValue(_maxMessagesOpt);
        var showKeys = parseResult.GetValue(_showKeysOpt);
        var showTimestamps = parseResult.GetValue(_showTimestampsOpt);
        var separator = parseResult.GetValue(_separatorOpt) ?? ":";
        var printOffset = parseResult.GetValue(_printOffsetOpt);
        var outputFile = parseResult.GetValue(_outputOpt);
        var outputFormat = parseResult.GetValue(_outputFormatOpt);
        var format = GetFormat(parseResult);

        // Auto-detect piped output - switch to plain format for piping
        var isPiped = Console.IsOutputRedirected();
        if (isPiped && format == OutputFormat.Table)
        {
            format = OutputFormat.Plain;
        }

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync(ct);

        // Determine starting offset
        long startOffset;
        if (offsetStr?.ToLowerInvariant() == "earliest")
        {
            startOffset = await client.Messaging.GetEarliestOffsetAsync(topic!, partition, ct);
        }
        else if (offsetStr?.ToLowerInvariant() == "latest")
        {
            startOffset = await client.Messaging.GetLatestOffsetAsync(topic!, partition, ct);
        }
        else if (long.TryParse(offsetStr, out var parsed))
        {
            startOffset = parsed;
        }
        else
        {
            startOffset = await client.Messaging.GetLatestOffsetAsync(topic!, partition, ct);
        }

        WriteVerbose(parseResult, $"Consuming from {topic}[{partition}] starting at offset {startOffset}...");

        // File output mode
        if (!string.IsNullOrEmpty(outputFile))
        {
            return await ConsumeToFileAsync(client, topic!, partition, startOffset, maxMessages,
                outputFile, outputFormat ?? "string", ct);
        }

        // Status messages to stderr when piped
        if (isPiped)
        {
            Console.WriteLineToError($"Consuming from '{topic}'[{partition}] starting at offset {startOffset}. Press Ctrl+C to exit.");
        }
        else if (format == OutputFormat.Table)
        {
            AnsiConsole.MarkupLine($"[dim]Consuming from '{topic}'[{partition}] starting at offset {startOffset}. Press Ctrl+C to exit.[/]");
            AnsiConsole.WriteLine();
        }

        var count = 0;
        var currentOffset = startOffset;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await client.Messaging.ReceiveAsync(topic!, partition, currentOffset, cancellationToken: ct);

                if (result.Messages.Count == 0)
                {
                    await Task.Delay(100, ct);
                    continue;
                }

                foreach (var msg in result.Messages)
                {
                    count++;
                    currentOffset = msg.Offset + 1;

                    if (format == OutputFormat.Json)
                    {
                        var output = new
                        {
                            Topic = topic,
                            Partition = partition,
                            Offset = msg.Offset,
                            Timestamp = msg.Timestamp,
                            Key = msg.KeyString,
                            Value = msg.ValueString
                        };
                        Console.WriteLine(JsonSerializer.Serialize(output));
                    }
                    else if (format == OutputFormat.Plain)
                    {
                        OutputPlainMessage(msg, showKeys, showTimestamps, printOffset, separator);
                    }
                    else
                    {
                        OutputTableMessage(msg, topic!, partition, showKeys, showTimestamps);
                    }

                    if (maxMessages > 0 && count >= maxMessages)
                        break;
                }

                if (maxMessages > 0 && count >= maxMessages)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
        }

        if (isPiped)
        {
            Console.WriteLineToError($"Consumed {count} message(s)");
        }
        else if (format == OutputFormat.Table)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[dim]Consumed {count} message(s)[/]");
        }
        return 0;
    }

    private async Task<int> ConsumeToFileAsync(
        SurgewaveNativeClient client, string topic, int partition, long startOffset,
        int maxMessages, string outputFile, string outputFormat, CancellationToken ct)
    {
        Console.WriteLineToError($"Consuming from '{topic}'[{partition}] to '{outputFile}' ({outputFormat})...");

        var count = 0;
        var currentOffset = startOffset;

        await using var fileStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write);
        await using var writer = outputFormat == "binary" ? null : new StreamWriter(fileStream, Encoding.UTF8);

        // For JSON array output, write opening bracket
        if (outputFormat == "json")
            await writer!.WriteLineAsync("[");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await client.Messaging.ReceiveAsync(topic, partition, currentOffset, cancellationToken: ct);

                if (result.Messages.Count == 0)
                {
                    await Task.Delay(100, ct);
                    continue;
                }

                foreach (var msg in result.Messages)
                {
                    count++;
                    currentOffset = msg.Offset + 1;

                    switch (outputFormat.ToLowerInvariant())
                    {
                        case "jsonl":
                            var jsonl = JsonSerializer.Serialize(new
                            {
                                offset = msg.Offset,
                                timestamp = msg.Timestamp,
                                key = msg.KeyString,
                                value = msg.ValueString
                            });
                            await writer!.WriteLineAsync(jsonl);
                            break;

                        case "json":
                            var comma = count > 1 ? "," : "";
                            var jsonObj = JsonSerializer.Serialize(new
                            {
                                offset = msg.Offset,
                                timestamp = msg.Timestamp,
                                key = msg.KeyString,
                                value = msg.ValueString
                            });
                            await writer!.WriteLineAsync($"{comma}{jsonObj}");
                            break;

                        case "binary":
                            if (msg.Value != null)
                                await fileStream.WriteAsync(msg.Value, ct);
                            break;

                        case "string":
                        default:
                            await writer!.WriteLineAsync(msg.ValueString);
                            break;
                    }

                    if (maxMessages > 0 && count >= maxMessages)
                        break;
                }

                if (maxMessages > 0 && count >= maxMessages)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
        }

        if (outputFormat == "json")
            await writer!.WriteLineAsync("]");

        WriteSuccess($"Consumed {count} message(s) to '{outputFile}'");
        return 0;
    }

    private static void OutputPlainMessage(
        ReceivedMessage msg, bool showKeys, bool showTimestamps,
        bool printOffset, string separator)
    {
        var output = new StringBuilder();

        if (printOffset)
            output.Append($"{msg.Offset}{separator}");

        if (showTimestamps)
        {
            var ts = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp);
            output.Append($"{ts:O}{separator}");
        }

        if (showKeys && msg.Key != null)
            output.Append($"{msg.KeyString}{separator}");

        output.Append(msg.ValueString);
        Console.WriteLine(output.ToString());
    }

    private static void OutputTableMessage(
        ReceivedMessage msg, string topic, int partition,
        bool showKeys, bool showTimestamps)
    {
        var prefix = new StringBuilder();

        if (showTimestamps)
        {
            var ts = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp);
            prefix.Append($"[dim]{ts:HH:mm:ss.fff}[/] ");
        }

        prefix.Append($"[cyan]{topic}[/][[{partition}]] @{msg.Offset}: ");

        if (showKeys && msg.Key != null)
            prefix.Append($"[yellow]{msg.KeyString}[/] = ");

        AnsiConsole.MarkupLine($"{prefix}{msg.ValueString}");
    }
}
