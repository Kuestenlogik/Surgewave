using System.CommandLine;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Config;

/// <summary>
/// Renders the effective Surgewave configuration for a given <c>appsettings.json</c>:
/// the user file with every installed plugin's <c>pluginsettings.json</c> layered
/// in beneath it (the same precedence order the broker uses at startup). With
/// <c>--explain</c>, every leaf value is annotated with the source it came from
/// — useful when a value is unexpected and you need to know whether it came from
/// the user config or a plugin default.
/// </summary>
internal sealed class ConfigViewCommand : Command
{
    private static readonly JsonSerializerOptions s_indented = new()
    {
        WriteIndented = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    public ConfigViewCommand() : base("view", "Render the broker's effective appsettings.json with every installed plugin's pluginsettings.json layered in (the same precedence the broker uses at startup) — for the CLI's own settings see `config show`")
    {
        var pathArg = new Argument<string?>("path")
        {
            Description = "Path to the appsettings.json file (default: ./appsettings.json)",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var assembliesOption = new Option<string?>("--assemblies", "-a")
        {
            Description = "Directory containing the plugins/ subdirectory (default: directory of the config file)",
        };
        var explainOption = new Option<bool>("--explain", "-e")
        {
            Description = "Annotate every leaf value with the source file it was read from",
        };
        var outputOption = new Option<string>("--output", "-o")
        {
            Description = "Output format: text (default, human-readable) or json (raw merged JSON, no headers — pipe-friendly)",
            DefaultValueFactory = _ => "text",
        };

        Arguments.Add(pathArg);
        Options.Add(assembliesOption);
        Options.Add(explainOption);
        Options.Add(outputOption);

        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var path = parseResult.GetValue(pathArg) ?? "appsettings.json";
            var assembliesDir = parseResult.GetValue(assembliesOption);
            var explain = parseResult.GetValue(explainOption);
            var output = parseResult.GetValue(outputOption) ?? "text";
            return Task.FromResult(Execute(path, assembliesDir, explain, output));
        });
    }

    private static int Execute(string configPath, string? assembliesDir, bool explain, string output)
    {
        var fullPath = Path.GetFullPath(configPath);
        if (!File.Exists(fullPath))
        {
            AnsiConsole.MarkupLine($"[red]Configuration file not found:[/] {fullPath}");
            return 1;
        }

        // Use ConfigLoader for loading + merging, with source tracking for --explain.
        var pluginsDir = ConfigLoader.ResolvePluginsDirectory(fullPath, assembliesDir);
        var sources = explain ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) : null;
        var (rootObject, pluginCount) = ConfigLoader.LoadAndMerge(fullPath, pluginsDir, sources);
        if (rootObject is null)
        {
            AnsiConsole.MarkupLine("[red]Top-level JSON value must be an object.[/]");
            return 1;
        }

        // Render
        if (output.Equals("json", StringComparison.OrdinalIgnoreCase))
        {
            System.Console.Out.WriteLine(rootObject.ToJsonString(s_indented));
        }
        else if (explain && sources is not null)
        {
            RenderExplain(fullPath, pluginCount, rootObject, sources);
        }
        else
        {
            RenderPlain(fullPath, pluginCount, rootObject);
        }
        return 0;
    }

    private static void RenderPlain(string configPath, int pluginCount, JsonObject merged)
    {
        AnsiConsole.MarkupLine($"[bold]Effective configuration:[/] {Markup.Escape(configPath)}");
        if (pluginCount > 0)
        {
            AnsiConsole.MarkupLine($"[dim]Layered with {pluginCount} plugin default(s).[/]");
        }
        AnsiConsole.WriteLine();

        var json = merged.ToJsonString(s_indented);
        AnsiConsole.WriteLine(json);
    }

    private static void RenderExplain(
        string configPath,
        int pluginCount,
        JsonObject merged,
        Dictionary<string, string> sources)
    {
        AnsiConsole.MarkupLine($"[bold]Effective configuration:[/] {Markup.Escape(configPath)}");
        if (pluginCount > 0)
        {
            AnsiConsole.MarkupLine($"[dim]Layered with {pluginCount} plugin default(s).[/]");
        }
        AnsiConsole.WriteLine();

        var rootBranch = new Tree($"[bold]Effective[/]");
        BuildExplainTree(rootBranch, merged, sources, prefix: "");
        AnsiConsole.Write(rootBranch);

        AnsiConsole.WriteLine();
        // Distinct source legend
        var distinctSources = sources.Values.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
        AnsiConsole.MarkupLine("[bold]Sources:[/]");
        foreach (var src in distinctSources)
        {
            AnsiConsole.MarkupLine($"  - {Markup.Escape(src)}");
        }
    }

    private static void BuildExplainTree(IHasTreeNodes parent, JsonObject obj, Dictionary<string, string> sources, string prefix)
    {
        foreach (var (key, value) in obj)
        {
            var path = string.IsNullOrEmpty(prefix) ? key : $"{prefix}:{key}";
            if (value is JsonObject child)
            {
                var node = parent.AddNode($"[cyan]{Markup.Escape(key)}[/]");
                BuildExplainTree(node, child, sources, path);
            }
            else if (value is JsonArray arr)
            {
                var sourceLabel = sources.TryGetValue(path, out var s) ? s : "?";
                var arrText = arr.ToJsonString();
                if (arrText.Length > 60) arrText = string.Concat(arrText.AsSpan(0, 57), "...");
                parent.AddNode($"[cyan]{Markup.Escape(key)}[/] = {Markup.Escape(arrText)} [dim][[from {Markup.Escape(sourceLabel)}]][/]");
            }
            else
            {
                var sourceLabel = sources.TryGetValue(path, out var s) ? s : "?";
                var valText = value?.ToJsonString() ?? "null";
                parent.AddNode($"[cyan]{Markup.Escape(key)}[/] = {Markup.Escape(valText)} [dim][[from {Markup.Escape(sourceLabel)}]][/]");
            }
        }
    }
}
