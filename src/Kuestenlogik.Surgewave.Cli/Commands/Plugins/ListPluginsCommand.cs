using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// List installed plugins (surgewave plugins list)
/// </summary>
public class ListPluginsCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory to scan",
        DefaultValueFactory = _ => "plugins"
    };

    public ListPluginsCommand() : base("list", "List installed plugins")
    {
        Aliases.Add("ls");
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var format = GetFormat(parseResult);

        var pluginsDir = Path.GetFullPath(directory);

        if (!Directory.Exists(pluginsDir))
        {
            WriteWarning($"Plugins directory does not exist: {pluginsDir}");
            return 0;
        }

        var manager = new PluginPackageManager();
        var plugins = new List<InstalledPlugin>();

        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            plugins.Add(plugin);
        }

        if (plugins.Count == 0)
        {
            WriteWarning("No plugins installed.");
            return 0;
        }

        if (format == OutputFormat.Json)
        {
            var json = JsonSerializer.Serialize(plugins.Select(p => new
            {
                p.Id,
                p.Name,
                p.Version,
                p.InstallPath
            }), JsonOptions);
            System.Console.WriteLine(json);
        }
        else if (format == OutputFormat.Plain)
        {
            foreach (var plugin in plugins)
            {
                System.Console.WriteLine($"{plugin.Id}\t{plugin.Version}");
            }
        }
        else
        {
            var table = new Table();
            table.AddColumn("ID");
            table.AddColumn("Name");
            table.AddColumn("Version");

            foreach (var plugin in plugins.OrderBy(p => p.Id))
            {
                table.AddRow(
                    plugin.Id,
                    plugin.Name,
                    plugin.Version);
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"\n[dim]Total: {plugins.Count} plugin(s)[/]");
        }

        return 0;
    }
}
