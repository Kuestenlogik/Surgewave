namespace Kuestenlogik.Surgewave.Cli.Commands.Groups;

/// <summary>
/// Command for managing consumer groups (surgewave groups ...)
/// </summary>
public class GroupsCommand : CommandBase
{
    public GroupsCommand() : base("groups", "Manage consumer groups")
    {
        Subcommands.Add(new ListGroupsCommand());
        Subcommands.Add(new DescribeGroupCommand());
        Subcommands.Add(new DeleteGroupCommand());
        Subcommands.Add(new LagCommand());
    }
}
