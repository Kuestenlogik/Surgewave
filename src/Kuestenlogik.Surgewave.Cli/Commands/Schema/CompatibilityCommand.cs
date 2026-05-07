using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Manage compatibility settings (surgewave schema compatibility)
/// </summary>
public class CompatibilityCommand : CommandBase
{
    public CompatibilityCommand() : base("compatibility", "Manage compatibility settings")
    {
        Subcommands.Add(new GetCompatibilityCommand());
        Subcommands.Add(new SetCompatibilityCommand());
        Subcommands.Add(new CheckCompatibilityCommand());
    }
}
