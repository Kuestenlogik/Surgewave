namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Command for managing cross-cluster replication (surgewave mirror ...)
/// </summary>
public class MirrorCommand : CommandBase
{
    public MirrorCommand() : base("mirror", "Manage mirror topics and cross-cluster replication")
    {
        Subcommands.Add(new CreateMirrorCommand());
        Subcommands.Add(new DescribeMirrorCommand());
        Subcommands.Add(new StatusMirrorCommand());
        Subcommands.Add(new PromoteMirrorCommand());
        Subcommands.Add(new FailoverMirrorCommand());
        Subcommands.Add(new PauseMirrorCommand());
        Subcommands.Add(new ResumeMirrorCommand());
        Subcommands.Add(new ListMirrorCommand());
        Subcommands.Add(new DeleteMirrorCommand());
    }
}
