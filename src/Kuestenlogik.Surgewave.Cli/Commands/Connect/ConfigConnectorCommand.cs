namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Manage connector config (surgewave connect config)
/// </summary>
public class ConfigConnectorCommand : CommandBase
{
    public ConfigConnectorCommand() : base("config", "Manage connector configuration")
    {
        Subcommands.Add(new GetConfigCommand());
        Subcommands.Add(new UpdateConfigCommand());
    }
}
