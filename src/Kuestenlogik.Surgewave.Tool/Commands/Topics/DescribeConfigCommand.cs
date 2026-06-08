using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Describe topic configuration (surgewave topics describe-config)
/// </summary>
public class DescribeConfigCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Name of the topic" };

    public DescribeConfigCommand() : base("describe-config", "Show topic configuration")
    {
        Arguments.Add(_topicArg);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            var config = await client.Topics.DescribeConfigAsync(topic, ct);

            if (format == OutputFormat.Json)
            {
                var output = new { Topic = topic, Config = config };
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var (key, value) in config.OrderBy(kv => kv.Key))
                    Console.WriteLine($"{key}\t{value}");
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Topic:[/] {topic}");
                AnsiConsole.MarkupLine($"[bold]Configuration:[/]");

                if (config.Count == 0)
                {
                    AnsiConsole.MarkupLine("  [dim](no custom configuration)[/]");
                }
                else
                {
                    var table = new Table();
                    table.AddColumn("Key");
                    table.AddColumn("Value");

                    foreach (var (key, value) in config.OrderBy(kv => kv.Key))
                    {
                        table.AddRow(key, value);
                    }

                    AnsiConsole.Write(table);
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe config: {ex.Message}");
            return 1;
        }
    }
}
