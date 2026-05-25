using System.CommandLine;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Acls;

internal static class AclsJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for managing ACLs (surgewave acls ...)
/// </summary>
public class AclsCommand : CommandBase
{
    public AclsCommand() : base("acls", "Manage access control lists (ACLs)")
    {
        Subcommands.Add(new ListAclsCommand());
        Subcommands.Add(new AddAclCommand());
        Subcommands.Add(new RemoveAclCommand());
        Subcommands.Add(new DescribeAclCommand());
    }
}
