using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Dlq;

/// <summary>
/// Command for managing Dead Letter Queues (surgewave dlq ...)
/// </summary>
public class DlqCommand : CommandBase
{
    public DlqCommand() : base("dlq", "Manage Dead Letter Queues")
    {
        Subcommands.Add(new ListDlqTopicsCommand());
        Subcommands.Add(new DescribeDlqCommand());
        Subcommands.Add(new ReplayDlqCommand());
        Subcommands.Add(new PurgeDlqCommand());
    }
}
