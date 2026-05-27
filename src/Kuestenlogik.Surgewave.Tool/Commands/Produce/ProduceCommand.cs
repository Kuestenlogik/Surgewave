using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Produce;

/// <summary>
/// Command for producing messages (surgewave produce)
/// Supports three modes:
/// 1. Single message: surgewave produce topic --value "message"
/// 2. Piped input: echo "message" | surgewave produce topic
/// 3. Interactive: surgewave produce topic --interactive
/// 4. File input: surgewave produce topic --input data.jsonl --input-format jsonl
/// </summary>
public class ProduceCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Topic to produce to" };
    private readonly Option<string?> _keyOpt = new("--key", "-k") { Description = "Message key (applies to all messages in piped mode)" };
    private readonly Option<string?> _valueOpt = new("--value", "-m") { Description = "Message value" };
    private readonly Option<int> _partitionOpt = new("--partition", "-p") { Description = "Target partition", DefaultValueFactory = _ => 0 };
    private readonly Option<bool> _interactiveOpt = new("--interactive", "-i") { Description = "Interactive mode with prompts", DefaultValueFactory = _ => false };
    private readonly Option<string> _separatorOpt = new("--separator", "-s") { Description = "Key-value separator for parsing input lines", DefaultValueFactory = _ => ":" };
    private readonly Option<bool> _parseKeyOpt = new("--parse-key") { Description = "Parse key from input lines using separator", DefaultValueFactory = _ => true };
    private readonly Option<string?> _inputOpt = new("--input") { Description = "Input file path (alternative to stdin or --value)" };
    private readonly Option<string> _inputFormatOpt = new("--input-format") { Description = "Input format: string, json, jsonl, binary", DefaultValueFactory = _ => "string" };

    public ProduceCommand() : base("produce", "Produce messages to a topic")
    {
        Arguments.Add(_topicArg);
        Options.Add(_keyOpt);
        Options.Add(_valueOpt);
        Options.Add(_partitionOpt);
        Options.Add(_interactiveOpt);
        Options.Add(_separatorOpt);
        Options.Add(_parseKeyOpt);
        Options.Add(_inputOpt);
        Options.Add(_inputFormatOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var key = parseResult.GetValue(_keyOpt);
        var value = parseResult.GetValue(_valueOpt);
        var partition = parseResult.GetValue(_partitionOpt);
        var interactive = parseResult.GetValue(_interactiveOpt);
        var separator = parseResult.GetValue(_separatorOpt);
        var parseKey = parseResult.GetValue(_parseKeyOpt);
        var inputFile = parseResult.GetValue(_inputOpt);
        var inputFormat = parseResult.GetValue(_inputFormatOpt);

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync(ct);

        // File input mode
        if (!string.IsNullOrEmpty(inputFile))
        {
            return await ProduceFromFileAsync(client, topic!, partition, key, inputFile, inputFormat ?? "string", ct);
        }

        // Single message mode
        if (!string.IsNullOrEmpty(value))
        {
            return await ProduceSingleMessageAsync(parseResult, client, topic!, partition, key, value, ct);
        }

        // Stdin mode
        if (interactive || Console.IsInputRedirected())
        {
            // Detect input format for piped input
            if (!inputFormat.Equals("string", StringComparison.OrdinalIgnoreCase) && Console.IsInputRedirected())
            {
                return await ProduceFromStdinWithFormatAsync(client, topic!, partition, key, inputFormat ?? "string", ct);
            }

            await ProduceFromStdinAsync(client, topic!, partition, key, separator ?? ":", parseKey, interactive, ct);
            return 0;
        }

        WriteError("No input provided. Use one of:");
        WriteError("  surgewave produce topic --value \"message\"");
        WriteError("  surgewave produce topic --input data.jsonl --input-format jsonl");
        WriteError("  echo \"message\" | surgewave produce topic");
        WriteError("  cat data.pb | surgewave produce topic --input-format binary");
        WriteError("  surgewave produce topic --interactive");
        return 1;
    }

    private async Task<int> ProduceFromFileAsync(
        SurgewaveNativeClient client, string topic, int partition,
        string? defaultKey, string filePath, string format, CancellationToken ct)
    {
        if (!File.Exists(filePath))
        {
            WriteError($"File not found: {filePath}");
            return 1;
        }

        var count = 0;

        switch (format.ToLowerInvariant())
        {
            case "jsonl":
                // Each line is a JSON record
                await foreach (var line in File.ReadLinesAsync(filePath, ct))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var (key, value) = ExtractKeyFromJson(line, defaultKey);
                    await client.Messaging.SendAsync(topic, partition, key, value, ct);
                    count++;
                }
                break;

            case "json":
                // Entire file is JSON — array = multiple records, object = single record
                var json = await File.ReadAllTextAsync(filePath, ct);
                count = await ProduceJsonAsync(client, topic, partition, defaultKey, json, ct);
                break;

            case "binary":
                // Entire file as one binary record
                var bytes = await File.ReadAllBytesAsync(filePath, ct);
                await client.Messaging.SendAsync(topic, partition,
                    defaultKey != null ? Encoding.UTF8.GetBytes(defaultKey) : null,
                    bytes, ct);
                count = 1;
                break;

            case "string":
            default:
                // Each line is a plain-text record
                await foreach (var line in File.ReadLinesAsync(filePath, ct))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    await client.Messaging.SendAsync(topic, partition, defaultKey, line, ct);
                    count++;
                }
                break;
        }

        WriteSuccess($"Produced {count} message(s) from '{filePath}' to '{topic}'");
        return 0;
    }

    private async Task<int> ProduceFromStdinWithFormatAsync(
        SurgewaveNativeClient client, string topic, int partition,
        string? defaultKey, string format, CancellationToken ct)
    {
        var count = 0;

        switch (format.ToLowerInvariant())
        {
            case "jsonl":
                string? line;
                while ((line = Console.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var (key, value) = ExtractKeyFromJson(line, defaultKey);
                    await client.Messaging.SendAsync(topic, partition, key, value, ct);
                    count++;
                }
                break;

            case "json":
                using (var reader = new StreamReader(System.Console.OpenStandardInput()))
                {
                    var json = await reader.ReadToEndAsync(ct);
                    count = await ProduceJsonAsync(client, topic, partition, defaultKey, json, ct);
                }
                break;

            case "binary":
                var stdin = System.Console.OpenStandardInput();
                await using (stdin)
                {
                    var ms = new MemoryStream();
                    await stdin.CopyToAsync(ms, ct);
                    await client.Messaging.SendAsync(topic, partition,
                        defaultKey != null ? Encoding.UTF8.GetBytes(defaultKey) : null,
                        ms.ToArray(), ct);
                    count = 1;
                }
                break;

            default:
                WriteError($"Unknown input format: {format}");
                return 1;
        }

        Console.WriteLineToError($"Produced {count} message(s) to '{topic}'");
        return 0;
    }

    private static async Task<int> ProduceJsonAsync(
        SurgewaveNativeClient client, string topic, int partition,
        string? defaultKey, string json, CancellationToken ct)
    {
        var count = 0;

        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var element in doc.RootElement.EnumerateArray())
            {
                var raw = element.GetRawText();
                var (key, value) = ExtractKeyFromJson(raw, defaultKey);
                await client.Messaging.SendAsync(topic, partition, key, value, ct);
                count++;
            }
        }
        else
        {
            var (key, value) = ExtractKeyFromJson(json, defaultKey);
            await client.Messaging.SendAsync(topic, partition, key, value, ct);
            count = 1;
        }

        return count;
    }

    private static (string? key, string value) ExtractKeyFromJson(string json, string? defaultKey)
    {
        // Try to extract "key" field from JSON object
        if (defaultKey == null)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty("key", out var keyProp))
                {
                    return (keyProp.GetString(), json);
                }
                if (doc.RootElement.TryGetProperty("id", out var idProp))
                {
                    return (idProp.ToString(), json);
                }
            }
            catch { /* not valid JSON, use as-is */ }
        }

        return (defaultKey, json);
    }

    private async Task ProduceFromStdinAsync(
        SurgewaveNativeClient client,
        string topic,
        int partition,
        string? defaultKey,
        string separator,
        bool parseKey,
        bool interactive,
        CancellationToken ct)
    {
        if (interactive && !Console.IsInputRedirected())
        {
            AnsiConsole.MarkupLine($"[dim]Producing to topic '{topic}'. Enter messages (Ctrl+C to exit):[/]");
            AnsiConsole.MarkupLine($"[dim]Format: [key{separator}]value[/]");
            AnsiConsole.WriteLine();
        }

        var count = 0;

        try
        {
            string? line;
            while (!ct.IsCancellationRequested && (line = Console.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                var (msgKey, msgValue) = ParseLine(line, defaultKey, separator, parseKey);

                await client.Messaging.SendAsync(topic, partition, msgKey, msgValue, ct);
                count++;
            }
        }
        catch (OperationCanceledException)
        {
            // Normal exit on Ctrl+C
        }

        Console.WriteLineToError($"Produced {count} message(s) to '{topic}'");
    }

    private static (string? key, string value) ParseLine(string line, string? defaultKey, string separator, bool parseKey)
    {
        if (!parseKey || string.IsNullOrEmpty(separator))
        {
            return (defaultKey, line);
        }

        var sepIndex = line.IndexOf(separator, StringComparison.Ordinal);
        if (sepIndex > 0)
        {
            return (line[..sepIndex], line[(sepIndex + separator.Length)..]);
        }

        return (defaultKey, line);
    }

    private async Task<int> ProduceSingleMessageAsync(
        ParseResult parseResult,
        SurgewaveNativeClient client,
        string topic,
        int partition,
        string? key,
        string value,
        CancellationToken ct)
    {
        try
        {
            var offset = await client.Messaging.SendAsync(topic, partition, key, value, ct);
            WriteSuccess($"Produced to {topic}[[{partition}]] @ offset {offset}");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to produce: {ex.Message}");
            return 1;
        }
    }
}
