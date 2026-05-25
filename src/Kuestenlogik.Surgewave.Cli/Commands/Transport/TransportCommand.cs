using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Transport;

/// <summary>
/// Command for transport operations (surgewave transport ...)
/// </summary>
public class TransportCommand : CommandBase
{
    public TransportCommand() : base("transport", "Transport layer diagnostics and configuration")
    {
        Subcommands.Add(new TransportStatusCommand());
        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
        // Subcommands.Add(new ShmInfoCommand());
        // Subcommands.Add(new ShmDiagnosticsCommand());
        Subcommands.Add(new ShmCleanupCommand());
    }
}
