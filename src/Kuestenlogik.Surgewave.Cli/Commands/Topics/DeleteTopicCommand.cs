using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Delete a topic (surgewave topics delete)
/// </summary>
public class DeleteTopicCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Name of the topic to delete" };

    public DeleteTopicCommand() : base("delete", "Delete a topic")
    {
        Arguments.Add(_topicArg);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);

        if (!ConfirmDestructive(parseResult, $"Delete topic '[cyan]{topic}[/]'? This cannot be undone."))
        {
            WriteWarning("Delete cancelled.");
            return 0;
        }

        WriteVerbose(parseResult, $"Deleting topic '{topic}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            await client.Topics.DeleteAsync(topic, ct);
            WriteSuccess($"Deleted topic '{topic}'");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete topic: {ex.Message}");
            return 1;
        }
    }
}
