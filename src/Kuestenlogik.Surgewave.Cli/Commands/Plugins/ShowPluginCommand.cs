using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Inspect a single installed plugin (surgewave plugin show &lt;id&gt;).
///
/// <para>
/// Renders the plugin's manifest fields, the bundled <c>pluginsettings.json</c>
/// content (or a notice if none is bundled), the list of assemblies and dependency
/// DLLs with their sizes, and the total install size on disk. Useful for inspecting
/// what was installed without unpacking the original <c>.swpkg</c>.
/// </para>
/// </summary>
public class ShowPluginCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Argument<string> _idArg = new("id")
    {
        Description = "Plugin ID (as listed by 'surgewave plugin list')",
    };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory to scan",
        DefaultValueFactory = _ => "plugins",
    };

    public ShowPluginCommand() : base("show", "Show details of an installed plugin (manifest, bundled settings, assemblies, install size)")
    {
        Arguments.Add(_idArg);
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var id = parseResult.GetValue(_idArg);
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var pluginsDir = Path.GetFullPath(directory);
        var format = GetFormat(parseResult);

        if (string.IsNullOrWhiteSpace(id))
        {
            WriteError("Plugin id is required.");
            return 1;
        }

        if (!Directory.Exists(pluginsDir))
        {
            WriteError($"Plugins directory does not exist: {pluginsDir}");
            return 1;
        }

        var manager = new PluginPackageManager();
        InstalledPlugin? match = null;
        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            if (string.Equals(plugin.Id, id, StringComparison.OrdinalIgnoreCase))
            {
                match = plugin;
                break;
            }
        }

        if (match is null)
        {
            WriteError($"Plugin '{id}' not installed in {pluginsDir}");
            return 1;
        }

        var manifest = match.Manifest!;
        var pluginDir = match.InstallPath!;

        // Discover bundled pluginsettings file via the manifest's pluginSettings field
        // (defaulting to "pluginsettings.json"). Skipped silently if not present.
        var settingsFileName = string.IsNullOrWhiteSpace(manifest.PluginSettings)
            ? "pluginsettings.json"
            : manifest.PluginSettings;
        var settingsPath = Path.Combine(pluginDir, settingsFileName);
        var hasSettings = File.Exists(settingsPath);
        var settingsJson = hasSettings ? await File.ReadAllTextAsync(settingsPath, ct) : null;

        // Enumerate every file in the plugin install dir for the size + assembly tables.
        var files = Directory.GetFiles(pluginDir, "*", SearchOption.AllDirectories)
            .Select(p => new FileInfo(p))
            .ToList();
        var totalBytes = files.Sum(f => f.Length);
        var assemblies = files.Where(f => f.Extension.Equals(".dll", StringComparison.OrdinalIgnoreCase)).ToList();

        if (format == OutputFormat.Json)
        {
            var payload = new
            {
                manifest.Id,
                manifest.Name,
                manifest.Version,
                manifest.Description,
                manifest.Authors,
                manifest.License,
                manifest.Tags,
                manifest.Assemblies,
                PluginSettingsFile = hasSettings ? settingsFileName : null,
                PluginSettings = hasSettings ? JsonDocument.Parse(settingsJson!).RootElement : (JsonElement?)null,
                InstallPath = pluginDir,
                TotalBytes = totalBytes,
                FileCount = files.Count,
                AssemblyCount = assemblies.Count,
            };
            System.Console.WriteLine(JsonSerializer.Serialize(payload, JsonOptions));
            return 0;
        }

        // Pretty (default) output
        AnsiConsole.Write(new Rule($"[bold cyan]{Markup.Escape(manifest.Name)}[/] [dim]({Markup.Escape(manifest.Id)})[/]").LeftJustified());

        var info = new Grid().AddColumn().AddColumn();
        info.AddRow("[bold]Version[/]", Markup.Escape(manifest.Version));
        if (!string.IsNullOrWhiteSpace(manifest.Description))
            info.AddRow("[bold]Description[/]", Markup.Escape(manifest.Description));
        if (manifest.Authors is { Length: > 0 })
            info.AddRow("[bold]Authors[/]", Markup.Escape(string.Join(", ", manifest.Authors)));
        if (!string.IsNullOrWhiteSpace(manifest.License))
            info.AddRow("[bold]License[/]", Markup.Escape(manifest.License));
        if (manifest.Tags is { Length: > 0 })
            info.AddRow("[bold]Tags[/]", Markup.Escape(string.Join(", ", manifest.Tags)));
        info.AddRow("[bold]Install path[/]", Markup.Escape(pluginDir));
        info.AddRow("[bold]Total size[/]", FormatBytes(totalBytes));
        info.AddRow("[bold]Files[/]", $"{files.Count} ({assemblies.Count} assemblies)");
        AnsiConsole.Write(info);
        AnsiConsole.WriteLine();

        // Assemblies + DLL deps table
        var asmTable = new Table().Title("[bold]Assemblies[/]")
            .AddColumn("File")
            .AddColumn(new TableColumn("Size").RightAligned())
            .AddColumn("Role");
        var manifestSet = new HashSet<string>(manifest.Assemblies, StringComparer.OrdinalIgnoreCase);
        foreach (var asm in assemblies.OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase))
        {
            var role = manifestSet.Contains(asm.Name) ? "[green]plugin[/]" : "[dim]dependency[/]";
            asmTable.AddRow(Markup.Escape(asm.Name), FormatBytes(asm.Length), role);
        }
        AnsiConsole.Write(asmTable);
        AnsiConsole.WriteLine();

        // Bundled plugin settings
        if (hasSettings)
        {
            AnsiConsole.MarkupLine($"[bold]Bundled defaults:[/] [dim]{Markup.Escape(settingsFileName)}[/]");
            AnsiConsole.WriteLine(settingsJson!);
        }
        else
        {
            AnsiConsole.MarkupLine("[dim]No bundled pluginsettings.json — plugin uses C# property defaults only.[/]");
        }

        return 0;
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024L) return $"{bytes / (1024.0 * 1024):F1} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F2} GB";
    }
}
