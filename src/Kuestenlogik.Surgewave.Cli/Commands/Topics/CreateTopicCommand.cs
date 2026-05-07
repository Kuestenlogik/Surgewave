using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Create a topic (surgewave topics create)
/// </summary>
public class CreateTopicCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Name of the topic to create" };
    private readonly Option<int> _partitionsOpt = new("--partitions", "-p") { Description = "Number of partitions", DefaultValueFactory = _ => 1 };
    private readonly Option<short> _replicationOpt = new("--replication-factor", "-r") { Description = "Replication factor", DefaultValueFactory = _ => (short)1 };

    public CreateTopicCommand() : base("create", "Create a new topic")
    {
        Arguments.Add(_topicArg);
        Options.Add(_partitionsOpt);
        Options.Add(_replicationOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var partitions = parseResult.GetValue(_partitionsOpt);
        var replication = parseResult.GetValue(_replicationOpt);

        WriteVerbose(parseResult, $"Creating topic '{topic}' with {partitions} partition(s)...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            await client.Topics.CreateAsync(topic, partitions, replication, ct);
            WriteSuccess($"Created topic '{topic}'");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to create topic: {ex.Message}");
            return 1;
        }
    }
}
