using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Serialization;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Messages;

/// <summary>
/// Command for fetching a single message by offset (surgewave message get).
/// Supports piping: surgewave message get topic 42 | jq .
/// Auto-detects piped output and switches to raw value bytes.
/// </summary>
public class GetMessageCommand : CommandBase
{
    private static readonly JsonSerializerOptions IndentedJson = new() { WriteIndented = true };

    private readonly Argument<string> _topicArg = new("topic") { Description = "Topic to fetch from" };
    private readonly Argument<long> _offsetArg = new("offset") { Description = "Message offset to fetch" };
    private readonly Option<int> _partitionOpt = new("--partition", "-p") { Description = "Partition (default: 0)", DefaultValueFactory = _ => 0 };
    private readonly Option<string> _formatOpt = new("--output-format") { Description = "Output format: raw, json, hex, base64 (default: raw)", DefaultValueFactory = _ => "raw" };
    private readonly Option<string?> _outputOpt = new("--output", "-o") { Description = "Write payload to file instead of stdout" };
    private readonly Option<bool> _headersOpt = new("--headers") { Description = "Include headers in output", DefaultValueFactory = _ => false };
    private readonly Option<bool> _decodeOpt = new("--decode") { Description = "Auto-detect and decode format (MessagePack->JSON, Schema Registry->JSON)", DefaultValueFactory = _ => false };

    public GetMessageCommand() : base("get", "Fetch a single message by offset")
    {
        Arguments.Add(_topicArg);
        Arguments.Add(_offsetArg);
        Options.Add(_partitionOpt);
        Options.Add(_formatOpt);
        Options.Add(_outputOpt);
        Options.Add(_headersOpt);
        Options.Add(_decodeOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg)!;
        var offset = parseResult.GetValue(_offsetArg);
        var partition = parseResult.GetValue(_partitionOpt);
        var format = parseResult.GetValue(_formatOpt) ?? "raw";
        var outputFile = parseResult.GetValue(_outputOpt);
        var showHeaders = parseResult.GetValue(_headersOpt);
        var decode = parseResult.GetValue(_decodeOpt);

        WriteVerbose(parseResult, $"Fetching message from {topic}[{partition}] at offset {offset}...");

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync(ct);

        var result = await client.Messaging.ReceiveAsync(topic, partition, offset, cancellationToken: ct);

        var msg = result.Messages.FirstOrDefault(m => m.Offset == offset);
        if (msg == null)
        {
            WriteError($"No message found at offset {offset} in {topic}[{partition}]");
            return 1;
        }

        // --output: write payload to file
        if (!string.IsNullOrEmpty(outputFile))
        {
            return await WriteToFileAsync(msg, outputFile, decode, ct);
        }

        var isPiped = System.Console.IsOutputRedirected;
        var formatExplicitlySet = parseResult.GetResult(_formatOpt) is not null;

        // Piped output (no --format explicitly set by user): raw value bytes to stdout
        if (isPiped && !formatExplicitlySet)
        {
            using var stdout = System.Console.OpenStandardOutput();
            await stdout.WriteAsync(msg.Value, ct);
            return 0;
        }

        return format.ToLowerInvariant() switch
        {
            "json" => OutputJson(msg, topic, partition, showHeaders, decode),
            "hex" => OutputHex(msg),
            "base64" => OutputBase64(msg),
            "raw" when isPiped => await OutputRawAsync(msg, ct),
            "raw" => OutputPretty(msg, topic, partition, showHeaders, decode),
            _ => OutputPretty(msg, topic, partition, showHeaders, decode)
        };
    }

    private static int OutputPretty(ReceivedMessage msg, string topic, int partition, bool showHeaders, bool decode)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp);

        var grid = new Grid();
        grid.AddColumn(new GridColumn().NoWrap().PadRight(2));
        grid.AddColumn(new GridColumn());

        grid.AddRow("[dim]Topic:[/]", $"[cyan]{Markup.Escape(topic)}[/]");
        grid.AddRow("[dim]Partition:[/]", partition.ToString());
        grid.AddRow("[dim]Offset:[/]", $"[bold]{msg.Offset}[/]");
        grid.AddRow("[dim]Timestamp:[/]", $"{timestamp:yyyy-MM-dd HH:mm:ss.fff zzz}");
        grid.AddRow("[dim]Key:[/]", msg.Key != null ? Markup.Escape(msg.KeyString!) : "[dim]null[/]");
        grid.AddRow("[dim]Size:[/]", $"{msg.Value.Length:N0} bytes");

        AnsiConsole.Write(new Panel(grid)
            .Header($"Message at offset {msg.Offset}")
            .BorderColor(Color.Cyan1)
            .Padding(1, 0));

        // Value
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[dim]Value:[/]");

        var displayValue = GetDisplayValue(msg, decode);
        AnsiConsole.Write(new Panel(new Text(displayValue))
            .BorderColor(Color.Grey)
            .Padding(1, 0));

        // Headers
        if (showHeaders && msg.Headers is { Count: > 0 })
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[dim]Headers:[/]");
            var table = new Table().AddColumn("Key").AddColumn("Value").BorderColor(Color.Grey);
            foreach (var (key, value) in msg.Headers)
            {
                table.AddRow(Markup.Escape(key), Markup.Escape(Encoding.UTF8.GetString(value)));
            }
            AnsiConsole.Write(table);
        }

        return 0;
    }

    private static int OutputJson(ReceivedMessage msg, string topic, int partition, bool showHeaders, bool decode)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp);
        var contentType = ContentTypeDetector.Detect(msg.Value);
        string? decodedValue = null;

        if (decode)
        {
            decodedValue = TryDecodeValue(msg.Value, contentType);
        }

        var envelope = new Dictionary<string, object?>
        {
            ["topic"] = topic,
            ["partition"] = partition,
            ["offset"] = msg.Offset,
            ["timestamp"] = timestamp,
            ["key"] = msg.KeyString,
            ["keyBase64"] = msg.Key != null ? Convert.ToBase64String(msg.Key) : null,
            ["value"] = decodedValue ?? msg.ValueString,
            ["valueBase64"] = Convert.ToBase64String(msg.Value),
            ["contentType"] = contentType,
            ["size"] = msg.Value.Length
        };

        if (showHeaders && msg.Headers is { Count: > 0 })
        {
            var headers = new Dictionary<string, string>();
            foreach (var (key, value) in msg.Headers)
            {
                headers[key] = Encoding.UTF8.GetString(value);
            }
            envelope["headers"] = headers;
        }

        var json = JsonSerializer.Serialize(envelope, IndentedJson);
        System.Console.WriteLine(json);
        return 0;
    }

    private static int OutputHex(ReceivedMessage msg)
    {
        var bytes = msg.Value;
        var sb = new StringBuilder();

        for (var i = 0; i < bytes.Length; i += 16)
        {
            sb.Append($"{i:X8}  ");

            // Hex bytes
            for (var j = 0; j < 16; j++)
            {
                if (i + j < bytes.Length)
                    sb.Append($"{bytes[i + j]:X2} ");
                else
                    sb.Append("   ");

                if (j == 7) sb.Append(' ');
            }

            sb.Append(" |");

            // ASCII
            for (var j = 0; j < 16 && i + j < bytes.Length; j++)
            {
                var b = bytes[i + j];
                sb.Append(b is >= 0x20 and < 0x7F ? (char)b : '.');
            }

            sb.AppendLine("|");
        }

        System.Console.Write(sb.ToString());
        return 0;
    }

    private static int OutputBase64(ReceivedMessage msg)
    {
        System.Console.WriteLine(Convert.ToBase64String(msg.Value));
        return 0;
    }

    private static async Task<int> OutputRawAsync(ReceivedMessage msg, CancellationToken ct)
    {
        using var stdout = System.Console.OpenStandardOutput();
        await stdout.WriteAsync(msg.Value, ct);
        return 0;
    }

    private static async Task<int> WriteToFileAsync(ReceivedMessage msg, string outputFile, bool decode, CancellationToken ct)
    {
        byte[] payload;

        if (decode)
        {
            var contentType = ContentTypeDetector.Detect(msg.Value);
            var decoded = TryDecodeValue(msg.Value, contentType);
            payload = decoded != null ? Encoding.UTF8.GetBytes(decoded) : msg.Value;
        }
        else
        {
            payload = msg.Value;
        }

        await File.WriteAllBytesAsync(outputFile, payload, ct);
        System.Console.Error.WriteLine($"Wrote {payload.Length:N0} bytes to {outputFile}");
        return 0;
    }

    private static string GetDisplayValue(ReceivedMessage msg, bool decode)
    {
        if (!decode) return msg.ValueString;

        var contentType = ContentTypeDetector.Detect(msg.Value);
        var decoded = TryDecodeValue(msg.Value, contentType);
        return decoded ?? msg.ValueString;
    }

    private static string? TryDecodeValue(byte[] payload, string contentType)
    {
        try
        {
            return contentType switch
            {
                ContentTypes.Json => FormatJson(payload),
                "application/x-confluent-schema" => FormatSchemaRegistryPayload(payload),
                "text/plain" => Encoding.UTF8.GetString(payload),
                _ => null
            };
        }
        catch
        {
            return null;
        }
    }

    private static string FormatJson(byte[] payload)
    {
        var doc = JsonDocument.Parse(payload);
        return JsonSerializer.Serialize(doc, IndentedJson);
    }

    private static string FormatSchemaRegistryPayload(byte[] payload)
    {
        if (payload.Length < 5) return Encoding.UTF8.GetString(payload);

        var schemaId = (payload[1] << 24) | (payload[2] << 16) | (payload[3] << 8) | payload[4];
        var dataPayload = payload.AsSpan(5);

        if (dataPayload.Length > 0 && (dataPayload[0] == '{' || dataPayload[0] == '['))
        {
            try
            {
                var jsonDoc = JsonDocument.Parse(dataPayload.ToArray());
                var json = JsonSerializer.Serialize(jsonDoc, IndentedJson);
                return $"// Schema ID: {schemaId}\n{json}";
            }
            catch { /* not JSON */ }
        }

        return $"// Schema ID: {schemaId}, Payload: {dataPayload.Length} bytes (binary)";
    }
}
