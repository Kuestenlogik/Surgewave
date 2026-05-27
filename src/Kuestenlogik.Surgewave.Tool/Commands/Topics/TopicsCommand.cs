using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Command for managing topics (surgewave topics ...)
/// </summary>
public class TopicsCommand : CommandBase
{
    public TopicsCommand() : base("topics", "Manage topics")
    {
        Subcommands.Add(new ListTopicsCommand());
        Subcommands.Add(new CreateTopicCommand());
        Subcommands.Add(new DeleteTopicCommand());
        Subcommands.Add(new DescribeTopicCommand());
        Subcommands.Add(new AlterConfigCommand());
        Subcommands.Add(new DescribeConfigCommand());
    }
}
