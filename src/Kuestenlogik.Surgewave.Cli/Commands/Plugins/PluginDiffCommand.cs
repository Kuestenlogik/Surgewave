using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Compare an installed plugin against a candidate <c>.swpkg</c> file
/// (surgewave plugin diff &lt;id&gt; &lt;package.swpkg&gt;).
///
/// <para>
/// Surfaces the operationally interesting differences before an upgrade:
/// version bump, manifest field changes, added/removed assemblies, and a
/// line-level diff of the bundled <c>pluginsettings.json</c>. Useful as a
/// pre-flight check during planned upgrade windows ("what am I about to
/// install over the existing version?") so operators can update their own
/// <c>appsettings.json</c> overrides ahead of time.
/// </para>
/// </summary>
public class PluginDiffCommand : CommandBase
{
    private static readonly JsonSerializerOptions s_indented = new() { WriteIndented = true };

    private readonly Argument<string> _idArg = new("id")
    {
        Description = "Plugin ID currently installed (as listed by 'surgewave plugin list')",
    };

    private readonly Argument<string> _packageArg = new("package")
    {
        Description = "Path to the candidate .swpkg file to compare against",
    };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory containing the installed version",
        DefaultValueFactory = _ => "plugins",
    };

    public PluginDiffCommand() : base("diff", "Diff an installed plugin against a candidate .swpkg file (manifest, assemblies, bundled defaults)")
    {
        Arguments.Add(_idArg);
        Arguments.Add(_packageArg);
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var id = parseResult.GetValue(_idArg);
        var packagePath = parseResult.GetValue(_packageArg);
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var pluginsDir = Path.GetFullPath(directory);

        if (string.IsNullOrWhiteSpace(id))
        {
            WriteError("Plugin id is required.");
            return 1;
        }
        if (string.IsNullOrWhiteSpace(packagePath) || !File.Exists(packagePath))
        {
            WriteError($"Package not found: {packagePath}");
            return 1;
        }
        if (!Directory.Exists(pluginsDir))
        {
            WriteError($"Plugins directory does not exist: {pluginsDir}");
            return 1;
        }

        // Load the installed manifest + bundled settings
        var manager = new PluginPackageManager();
        InstalledPlugin? installed = null;
        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            if (string.Equals(plugin.Id, id, StringComparison.OrdinalIgnoreCase))
            {
                installed = plugin;
                break;
            }
        }
        if (installed is null)
        {
            WriteError($"Plugin '{id}' not installed in {pluginsDir}");
            return 1;
        }
        var installedManifest = installed.Manifest!;
        var installedDir = installed.InstallPath!;
        var (installedSettingsName, installedSettingsContent) = LoadInstalledSettings(installedDir, installedManifest);
        var installedAssemblies = Directory.GetFiles(installedDir, "*.dll", SearchOption.AllDirectories)
            .Select(p => Path.GetFileName(p))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Load the candidate package's manifest + bundled settings + assemblies (without
        // installing it). The .swpkg is a zip; we read entries directly so the diff is
        // non-destructive.
        var (candidateManifest, candidateSettingsName, candidateSettingsContent, candidateAssemblies) =
            await ReadCandidateAsync(packagePath, ct);

        if (!string.Equals(candidateManifest.Id, installedManifest.Id, StringComparison.OrdinalIgnoreCase))
        {
            WriteError($"Candidate plugin id '{candidateManifest.Id}' does not match installed id '{installedManifest.Id}'");
            return 1;
        }

        // ---- Header ----
        AnsiConsole.Write(new Rule($"[bold cyan]{Markup.Escape(installedManifest.Name)}[/] [dim]({Markup.Escape(installedManifest.Id)})[/]").LeftJustified());
        AnsiConsole.MarkupLine($"[bold]Installed:[/] {Markup.Escape(installedManifest.Version)}     [bold]Candidate:[/] {Markup.Escape(candidateManifest.Version)}");
        AnsiConsole.WriteLine();

        // ---- Manifest field-by-field diff ----
        AnsiConsole.MarkupLine("[bold]Manifest changes[/]");
        var manifestTable = new Table()
            .AddColumn("Field")
            .AddColumn("Installed")
            .AddColumn("Candidate");
        AddManifestDiff(manifestTable, "version", installedManifest.Version, candidateManifest.Version);
        AddManifestDiff(manifestTable, "description", installedManifest.Description, candidateManifest.Description);
        AddManifestDiff(manifestTable, "license", installedManifest.License, candidateManifest.License);
        AddManifestDiff(manifestTable, "minRuntimeVersion", installedManifest.MinRuntimeVersion, candidateManifest.MinRuntimeVersion);
        AddManifestDiff(manifestTable, "pluginSettings", installedManifest.PluginSettings, candidateManifest.PluginSettings);
        AddManifestDiff(manifestTable, "tags", FormatList(installedManifest.Tags), FormatList(candidateManifest.Tags));
        AnsiConsole.Write(manifestTable);
        AnsiConsole.WriteLine();

        // ---- Assembly add/remove ----
        var added = candidateAssemblies.Except(installedAssemblies, StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
        var removed = installedAssemblies.Except(candidateAssemblies, StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
        if (added.Count > 0 || removed.Count > 0)
        {
            AnsiConsole.MarkupLine("[bold]Assembly changes[/]");
            foreach (var f in added) AnsiConsole.MarkupLine($"  [green]+ {Markup.Escape(f)}[/]");
            foreach (var f in removed) AnsiConsole.MarkupLine($"  [red]- {Markup.Escape(f)}[/]");
            AnsiConsole.WriteLine();
        }
        else
        {
            AnsiConsole.MarkupLine("[bold]Assembly changes[/]: [dim]none[/]");
            AnsiConsole.WriteLine();
        }

        // ---- pluginsettings.json line diff ----
        AnsiConsole.MarkupLine("[bold]Bundled defaults changes[/]");
        if (installedSettingsContent is null && candidateSettingsContent is null)
        {
            AnsiConsole.MarkupLine("  [dim]neither version bundles a settings file[/]");
        }
        else if (installedSettingsContent is null)
        {
            AnsiConsole.MarkupLine($"  [green]+ candidate adds {Markup.Escape(candidateSettingsName ?? "(unknown)")}[/]");
            PrintSettings(candidateSettingsContent!, prefix: "    ", color: "green");
        }
        else if (candidateSettingsContent is null)
        {
            AnsiConsole.MarkupLine($"  [red]- candidate removes {Markup.Escape(installedSettingsName ?? "(unknown)")}[/]");
        }
        else
        {
            // Render a unified-style line diff. Both files are typically small JSON
            // objects, so a per-line comparison is plenty without a real diff library.
            PrintLineDiff(installedSettingsContent, candidateSettingsContent);
        }

        return 0;
    }

    private static (string? name, string? content) LoadInstalledSettings(string pluginDir, PluginManifest manifest)
    {
        var fileName = string.IsNullOrWhiteSpace(manifest.PluginSettings) ? "pluginsettings.json" : manifest.PluginSettings;
        var path = Path.Combine(pluginDir, fileName);
        return File.Exists(path) ? (fileName, File.ReadAllText(path)) : ((string?)null, (string?)null);
    }

    private static async Task<(PluginManifest manifest, string? settingsName, string? settingsContent, HashSet<string> assemblies)>
        ReadCandidateAsync(string packagePath, CancellationToken ct)
    {
        await using var stream = File.OpenRead(packagePath);
        using var archive = new ZipArchive(stream, ZipArchiveMode.Read);

        // Manifest
        var manifestEntry = archive.GetEntry("plugin.json")
            ?? throw new InvalidOperationException($"Candidate package does not contain plugin.json: {packagePath}");
        PluginManifest manifest;
        await using (var ms = manifestEntry.Open())
        {
            manifest = await JsonSerializer.DeserializeAsync<PluginManifest>(ms, cancellationToken: ct)
                ?? throw new InvalidOperationException("Failed to parse candidate plugin.json");
        }

        // Bundled settings — name comes from the manifest, default to "pluginsettings.json"
        var settingsName = string.IsNullOrWhiteSpace(manifest.PluginSettings) ? "pluginsettings.json" : manifest.PluginSettings;
        string? settingsContent = null;
        var settingsEntry = archive.GetEntry(settingsName);
        if (settingsEntry != null)
        {
            await using var ss = settingsEntry.Open();
            using var reader = new StreamReader(ss);
            settingsContent = await reader.ReadToEndAsync(ct);
        }

        // Assemblies — every .dll under lib/ or deps/
        var assemblies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in archive.Entries)
        {
            if (entry.FullName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
            {
                assemblies.Add(Path.GetFileName(entry.FullName));
            }
        }

        return (manifest, settingsContent != null ? settingsName : null, settingsContent, assemblies);
    }

    private static void AddManifestDiff(Table table, string field, string? installed, string? candidate)
    {
        if (string.Equals(installed, candidate, StringComparison.Ordinal)) return;
        table.AddRow(
            $"[bold]{Markup.Escape(field)}[/]",
            installed is null ? "[dim](unset)[/]" : Markup.Escape(installed),
            candidate is null ? "[dim](unset)[/]" : $"[yellow]{Markup.Escape(candidate)}[/]");
    }

    private static string? FormatList(string[]? items)
        => items is null || items.Length == 0 ? null : string.Join(", ", items);

    private static void PrintSettings(string json, string prefix, string color)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var formatted = JsonSerializer.Serialize(doc.RootElement, s_indented);
            foreach (var line in formatted.Split('\n'))
            {
                AnsiConsole.MarkupLine($"{prefix}[{color}]{Markup.Escape(line.TrimEnd('\r'))}[/]");
            }
        }
        catch
        {
            AnsiConsole.MarkupLine($"{prefix}[{color}]{Markup.Escape(json)}[/]");
        }
    }

    private static void PrintLineDiff(string installed, string candidate)
    {
        // Normalise both sides through JsonDocument so whitespace differences don't
        // generate noise in the diff. Then a per-line set comparison flags additions
        // and removals — full LCS diff is overkill for files this small.
        var installedLines = NormalizeJson(installed);
        var candidateLines = NormalizeJson(candidate);

        var installedSet = new HashSet<string>(installedLines, StringComparer.Ordinal);
        var candidateSet = new HashSet<string>(candidateLines, StringComparer.Ordinal);

        if (installedSet.SetEquals(candidateSet))
        {
            AnsiConsole.MarkupLine("  [dim]no changes[/]");
            return;
        }

        // Walk the candidate to preserve order
        foreach (var line in candidateLines)
        {
            if (installedSet.Contains(line))
            {
                AnsiConsole.MarkupLine($"    {Markup.Escape(line)}");
            }
            else
            {
                AnsiConsole.MarkupLine($"  [green]+ {Markup.Escape(line)}[/]");
            }
        }
        // Then any installed lines not in the candidate
        foreach (var line in installedLines.Where(l => !candidateSet.Contains(l)))
        {
            AnsiConsole.MarkupLine($"  [red]- {Markup.Escape(line)}[/]");
        }
    }

    private static string[] NormalizeJson(string content)
    {
        try
        {
            using var doc = JsonDocument.Parse(content);
            var formatted = JsonSerializer.Serialize(doc.RootElement, s_indented);
            return formatted.Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
        }
        catch
        {
            return content.Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
        }
    }
}
