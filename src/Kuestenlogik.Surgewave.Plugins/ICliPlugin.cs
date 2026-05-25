using System.CommandLine;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that contributes CLI commands to the <c>surgewave</c> CLI tool.
/// Discovered at startup from assemblies in the <c>plugins/</c> directory.
/// Each plugin returns one or more top-level commands (e.g. <c>surgewave fleet</c>).
/// </summary>
public interface ICliPlugin : IPlugin
{
    /// <summary>
    /// Returns the CLI commands this plugin provides.
    /// Each command becomes a top-level subcommand of <c>surgewave</c>.
    /// </summary>
    IEnumerable<Command> GetCommands();
}
