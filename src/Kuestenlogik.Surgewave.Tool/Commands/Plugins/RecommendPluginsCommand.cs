using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Packaging.Recommendations;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Show plugins curated by Surgewave Core (e.g. AI Basics) that the operator
/// has not yet installed, with an opt-in <c>--apply</c> flag that runs
/// <c>install</c> for each one. The catalog ships embedded in the CLI
/// binary — no network round-trip required.
/// </summary>
public sealed class RecommendPluginsCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory to scan for already-installed plugins.",
        DefaultValueFactory = _ => "plugins",
    };

    private readonly Option<string?> _categoryOpt = new("--category", "-c")
    {
        Description = "Filter recommendations by category (e.g. 'ai'). Default: all categories.",
    };

    private readonly Option<bool> _applyOpt = new("--apply")
    {
        Description = "Install missing recommended plugins. Prompts for confirmation per plugin unless --yes is set.",
    };

    public RecommendPluginsCommand()
        : base("recommend", "List curated plugin recommendations and (optionally) install the missing ones.")
    {
        Options.Add(_directoryOpt);
        Options.Add(_categoryOpt);
        Options.Add(_applyOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var category = parseResult.GetValue(_categoryOpt);
        var apply = parseResult.GetValue(_applyOpt);
        var format = GetFormat(parseResult);

        var pluginsDir = Path.GetFullPath(directory);

        var catalog = RecommendedPluginCatalog.Load();
        if (!string.IsNullOrWhiteSpace(category))
        {
            catalog = catalog
                .Where(p => p.Category.Equals(category, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        if (catalog.Count == 0)
        {
            WriteWarning(string.IsNullOrWhiteSpace(category)
                ? "Recommendation catalog is empty."
                : $"No recommendations match category '{category}'.");
            return 0;
        }

        var installedIds = await GetInstalledIdsAsync(pluginsDir, ct);

        var rows = catalog
            .Select(p => new RecommendationRow(p, installedIds.Contains(p.Id)))
            .ToList();

        if (format == OutputFormat.Json)
        {
            var json = JsonSerializer.Serialize(rows.Select(r => new
            {
                r.Plugin.Id,
                r.Plugin.DisplayName,
                r.Plugin.Category,
                r.Plugin.Description,
                r.Plugin.ProjectUrl,
                r.IsInstalled,
            }), JsonOptions);
            System.Console.WriteLine(json);
            return 0;
        }

        if (format == OutputFormat.Plain)
        {
            foreach (var r in rows)
            {
                var state = r.IsInstalled ? "installed" : "missing";
                System.Console.WriteLine($"{r.Plugin.Id}\t{r.Plugin.Category}\t{state}");
            }
            return 0;
        }

        RenderTable(rows);

        if (!apply)
        {
            var missing = rows.Where(r => !r.IsInstalled).ToList();
            if (missing.Count > 0)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]{missing.Count} recommended plugin(s) missing. Re-run with [bold]--apply[/] to install.[/]");
            }
            return 0;
        }

        return await ApplyAsync(rows, parseResult, ct);
    }

    private static void RenderTable(IReadOnlyList<RecommendationRow> rows)
    {
        var table = new Table().Border(TableBorder.Simple);
        table.AddColumn("Plugin");
        table.AddColumn("Category");
        table.AddColumn("Status");
        table.AddColumn("Why recommended");

        foreach (var r in rows)
        {
            var status = r.IsInstalled
                ? "[green]installed[/]"
                : "[yellow]missing[/]";
            table.AddRow(
                $"[bold]{Markup.Escape(r.Plugin.DisplayName)}[/]\n[dim]{Markup.Escape(r.Plugin.Id)}[/]",
                Markup.Escape(r.Plugin.Category),
                status,
                Markup.Escape(r.Plugin.WhyRecommended));
        }

        AnsiConsole.Write(table);
    }

    private async Task<int> ApplyAsync(
        IReadOnlyList<RecommendationRow> rows,
        ParseResult parseResult,
        CancellationToken ct)
    {
        var missing = rows.Where(r => !r.IsInstalled).ToList();
        if (missing.Count == 0)
        {
            AnsiConsole.WriteLine();
            WriteSuccess("All recommended plugins are already installed.");
            return 0;
        }

        var failed = 0;
        foreach (var row in missing)
        {
            AnsiConsole.WriteLine();
            var question = $"Install [bold]{Markup.Escape(row.Plugin.DisplayName)}[/] ([dim]{Markup.Escape(row.Plugin.Id)}[/])?";
            if (!ConfirmDestructive(parseResult, question))
            {
                AnsiConsole.MarkupLine($"  [dim]skipped[/]");
                continue;
            }

            var args = BuildInstallArgs(row.Plugin);
            AnsiConsole.MarkupLine($"  [dim]→ surgewave {string.Join(" ", args)}[/]");

            var exit = await RunSurgewaveAsync(args, ct);
            if (exit != 0)
            {
                WriteError($"Install failed for {row.Plugin.Id} (exit {exit}).");
                failed++;
            }
        }

        AnsiConsole.WriteLine();
        if (failed == 0)
        {
            WriteSuccess("All missing recommendations installed.");
            return 0;
        }

        WriteWarning($"{failed} install(s) failed. Re-run individually with `surgewave plugins install` for details.");
        return 1;
    }

    private static string[] BuildInstallArgs(RecommendedPlugin plugin)
    {
        var args = new List<string> { "plugins", "install", plugin.Id };
        if (!string.IsNullOrWhiteSpace(plugin.Source))
        {
            args.Add("--source");
            args.Add(plugin.Source);
        }
        return args.ToArray();
    }

    private static async Task<int> RunSurgewaveAsync(string[] args, CancellationToken ct)
    {
        var entry = Environment.ProcessPath
            ?? throw new InvalidOperationException("Could not resolve current process path.");
        var psi = new ProcessStartInfo
        {
            FileName = entry,
            UseShellExecute = false,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
        };
        foreach (var arg in args) psi.ArgumentList.Add(arg);

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException($"Could not spawn {entry}.");
        await proc.WaitForExitAsync(ct).ConfigureAwait(false);
        return proc.ExitCode;
    }

    private static async Task<HashSet<string>> GetInstalledIdsAsync(string pluginsDir, CancellationToken ct)
    {
        var installed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!Directory.Exists(pluginsDir))
        {
            return installed;
        }

        var manager = new PluginPackageManager();
        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            installed.Add(plugin.Id);
        }
        return installed;
    }

    private sealed record RecommendationRow(RecommendedPlugin Plugin, bool IsInstalled);
}
