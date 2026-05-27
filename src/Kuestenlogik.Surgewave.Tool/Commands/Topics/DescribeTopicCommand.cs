using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Describe a topic (surgewave topics describe)
/// </summary>
public class DescribeTopicCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Name of the topic to describe" };

    public DescribeTopicCommand() : base("describe", "Describe a topic")
    {
        Aliases.Add("show");
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
            var topics = await client.Topics.ListAsync(ct);
            var topicInfo = topics.FirstOrDefault(t => t.Name == topic);

            if (topicInfo == null)
            {
                WriteError($"Topic '{topic}' not found");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                var partitions = new List<object>();
                for (int i = 0; i < topicInfo.PartitionCount; i++)
                {
                    var offset = await client.Messaging.GetLatestOffsetAsync(topic, i, ct);
                    partitions.Add(new { Partition = i, HighWatermark = offset });
                }

                var info = new
                {
                    topicInfo.Name,
                    topicInfo.PartitionCount,
                    Partitions = partitions
                };
                Console.WriteLine(JsonSerializer.Serialize(info, JsonOptions.Indented));
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Topic:[/] {topicInfo.Name}");
                AnsiConsole.MarkupLine($"[bold]Partitions:[/] {topicInfo.PartitionCount}");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Partition");
                table.AddColumn("High Watermark");

                for (int i = 0; i < topicInfo.PartitionCount; i++)
                {
                    var offset = await client.Messaging.GetLatestOffsetAsync(topic, i, ct);
                    table.AddRow(i.ToString(), offset.ToString());
                }

                AnsiConsole.Write(table);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe topic: {ex.Message}");
            return 1;
        }
    }
}
