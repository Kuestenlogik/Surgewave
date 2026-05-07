using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Cluster;

/// <summary>
/// Command for cluster operations (surgewave cluster ...)
/// </summary>
public class ClusterCommand : CommandBase
{
    public ClusterCommand() : base("cluster", "Cluster operations and balancing")
    {
        Subcommands.Add(new ClusterStatusCommand());
        Subcommands.Add(new ClusterNodesCommand());
        Subcommands.Add(new ClusterRaftCommand());
        Subcommands.Add(new ClusterPartitionsCommand());
        Subcommands.Add(new ClusterBalanceCommand());
    }
}
