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

    private readonly Option<string?> _directoryOpt = new("--directory", "-d")
    {
        Description = "List one explicit directory instead of every scope"
    };


    public ListPluginsCommand() : base("list", "List installed plugins")
    {
        Aliases.Add("ls");
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var directory = parseResult.GetValue(_directoryOpt);
        var format = GetFormat(parseResult);

        // Listing used to read "plugins" relative to the working directory, i.e. a
        // directory the broker never reads — so it confirmed installs the broker
        // would never see (#157). It now walks the same scopes the broker does, in
        // the same order, and names them: the previous output mentioned no path at
        // all, which is why the mismatch was invisible from both ends.
        var directories = string.IsNullOrWhiteSpace(directory)
            ? SurgewavePluginDirectories.SearchOrder()
            : [Path.GetFullPath(directory)];

        var manager = new PluginPackageManager();
        var plugins = new List<InstalledPlugin>();
        var origin = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var scanned = new List<string>();

        foreach (var dir in directories)
        {
            if (!Directory.Exists(dir)) continue;
            scanned.Add(dir);

            await foreach (var plugin in manager.GetInstalledPluginsAsync(dir, ct))
            {
                // Later scopes win, matching the broker's own precedence, so what is
                // listed is what would actually load.
                var shadowed = plugins.FindIndex(p =>
                    string.Equals(p.Id, plugin.Id, StringComparison.OrdinalIgnoreCase));
                if (shadowed >= 0) plugins.RemoveAt(shadowed);

                plugins.Add(plugin);
                origin[plugin.Id] = dir;
            }
        }

        if (plugins.Count == 0)
        {
            WriteWarning("No plugins installed.");
            foreach (var dir in directories)
                WriteMarkup($"[dim]  searched: {dir}{(Directory.Exists(dir) ? "" : "  (does not exist)")}[/]");
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
            table.AddColumn("Scope");

            foreach (var plugin in plugins.OrderBy(p => p.Id))
            {
                table.AddRow(
                    plugin.Id,
                    plugin.Name,
                    plugin.Version,
                    origin.TryGetValue(plugin.Id, out var dir) ? DescribeScope(dir) : "?");
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"\n[dim]Total: {plugins.Count} plugin(s)[/]");
            foreach (var scannedDir in scanned)
                AnsiConsole.MarkupLine($"[dim]  {DescribeScope(scannedDir)}: {scannedDir}[/]");
        }

        return 0;
    }

    /// <summary>
    /// Names the scope a directory belongs to. Printing the bare path would already
    /// beat printing nothing, but the scope answers the question an operator
    /// actually has: whether the broker sees it whichever account it runs under.
    /// </summary>
    private static string DescribeScope(string directory)
    {
        if (PathsEqual(directory, SurgewavePluginDirectories.Installation)) return "installation";
        if (PathsEqual(directory, SurgewavePluginDirectories.Machine)) return "machine";
        if (PathsEqual(directory, SurgewavePluginDirectories.User)) return "user";
        return "explicit";
    }

    private static bool PathsEqual(string a, string b) =>
        string.Equals(Path.GetFullPath(a), Path.GetFullPath(b), StringComparison.OrdinalIgnoreCase);
}
