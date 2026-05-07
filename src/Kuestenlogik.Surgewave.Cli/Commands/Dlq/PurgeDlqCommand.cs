using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

/// <summary>
/// Purge (delete) messages from a DLQ topic (surgewave dlq purge)
/// </summary>
public class PurgeDlqCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("dlq-topic")
    {
        Description = "Name of the DLQ topic to purge"
    };

    private readonly Option<bool> _forceOpt = new("--force", "-f")
    {
        Description = "Skip confirmation prompt",
        DefaultValueFactory = _ => false
    };

    private readonly Option<bool> _deleteTopicOpt = new("--delete-topic")
    {
        Description = "Delete the entire DLQ topic instead of just purging messages",
        DefaultValueFactory = _ => false
    };

    public PurgeDlqCommand() : base("purge", "Purge messages from a DLQ topic")
    {
        Arguments.Add(_topicArg);
        Options.Add(_forceOpt);
        Options.Add(_deleteTopicOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var topic = parseResult.GetValue(_topicArg);
        var force = parseResult.GetValue(_forceOpt);
        var deleteTopic = parseResult.GetValue(_deleteTopicOpt);

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

            // Get message count
            long totalMessages = 0;
            for (int p = 0; p < topicInfo.PartitionCount; p++)
            {
                var earliest = await client.Messaging.GetEarliestOffsetAsync(topic!, p, ct);
                var latest = await client.Messaging.GetLatestOffsetAsync(topic!, p, ct);
                totalMessages += latest - earliest;
            }

            // Confirm unless --force
            if (!force && format != OutputFormat.Json && !System.Console.IsInputRedirected)
            {
                var action = deleteTopic ? "DELETE" : "PURGE";
                var prompt = deleteTopic
                    ? $"Are you sure you want to [red]DELETE[/] the topic '{topic}'?"
                    : $"Are you sure you want to [yellow]PURGE[/] {totalMessages} message(s) from '{topic}'?";

                if (!AnsiConsole.Confirm(prompt, false))
                {
                    AnsiConsole.MarkupLine("[dim]Operation cancelled.[/]");
                    return 0;
                }
            }

            if (deleteTopic)
            {
                // Delete the entire topic
                await client.Topics.DeleteAsync(topic!, ct);

                if (format == OutputFormat.Json)
                {
                    var output = new { Topic = topic, Action = "deleted", MessagesRemoved = totalMessages };
                    Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
                }
                else
                {
                    WriteSuccess($"Deleted DLQ topic '{topic}' ({totalMessages} messages removed)");
                }
            }
            else
            {
                // Purge by deleting and recreating the topic
                var partitionCount = topicInfo.PartitionCount;
                await client.Topics.DeleteAsync(topic!, ct);
                await client.Topics.CreateAsync(topic!, partitionCount, replicationFactor: 1, ct);

                if (format == OutputFormat.Json)
                {
                    var output = new { Topic = topic, Action = "purged", MessagesRemoved = totalMessages, PartitionsPreserved = partitionCount };
                    Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
                }
                else
                {
                    WriteSuccess($"Purged {totalMessages} message(s) from '{topic}'");
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to purge DLQ: {ex.Message}");
            return 1;
        }
    }
}
