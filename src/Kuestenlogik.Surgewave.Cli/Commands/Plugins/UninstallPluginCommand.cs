using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Uninstall a plugin (surgewave plugins uninstall)
/// </summary>
public class UninstallPluginCommand : CommandBase
{
    private readonly Argument<string> _pluginIdArg = new("pluginId")
    {
        Description = "ID of the plugin to uninstall"
    };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory",
        DefaultValueFactory = _ => "plugins"
    };

    public UninstallPluginCommand() : base("uninstall", "Uninstall a plugin")
    {
        Arguments.Add(_pluginIdArg);
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var pluginId = parseResult.GetValue(_pluginIdArg);
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";

        if (string.IsNullOrEmpty(pluginId))
        {
            WriteError("Plugin ID is required.");
            return 1;
        }

        var pluginsDir = Path.GetFullPath(directory);
        var manager = new PluginPackageManager();

        // Check if plugin exists
        var pluginDir = Path.Combine(pluginsDir, pluginId);
        if (!Directory.Exists(pluginDir))
        {
            WriteError($"Plugin not found: {pluginId}");
            WriteMarkup("[dim]Use 'surgewave plugins list' to see installed plugins.[/]");
            return 1;
        }

        // Confirm uninstallation
        if (!AnsiConsole.Confirm($"Uninstall plugin [yellow]{pluginId}[/]?", defaultValue: false))
        {
            WriteWarning("Cancelled.");
            return 0;
        }

        var result = await manager.UninstallAsync(pluginId, pluginsDir);

        if (result)
        {
            WriteSuccess($"Uninstalled {pluginId}");
            return 0;
        }
        else
        {
            WriteError($"Failed to uninstall {pluginId}");
            return 1;
        }
    }
}
