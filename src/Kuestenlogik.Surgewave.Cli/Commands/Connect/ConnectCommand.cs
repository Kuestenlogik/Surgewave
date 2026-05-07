using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

internal static class ConnectJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for managing Kafka Connect (surgewave connect ...)
/// </summary>
public class ConnectCommand : CommandBase
{
    public ConnectCommand() : base("connect", "Manage Kafka Connect connectors")
    {
        Subcommands.Add(new ListConnectorsCommand());
        Subcommands.Add(new CreateConnectorCommand());
        Subcommands.Add(new DescribeConnectorCommand());
        Subcommands.Add(new DeleteConnectorCommand());
        Subcommands.Add(new StatusConnectorCommand());
        Subcommands.Add(new ConfigConnectorCommand());
        Subcommands.Add(new RestartConnectorCommand());
        Subcommands.Add(new PauseConnectorCommand());
        Subcommands.Add(new ResumeConnectorCommand());
        Subcommands.Add(new TasksCommand());
        Subcommands.Add(new PluginsCommand());
    }
}
