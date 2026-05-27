using System.CommandLine;
using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Config;

/// <summary>
/// Validates a Surgewave <c>appsettings.json</c> against every <see cref="IValidatableConfig"/>
/// type discoverable in the assemblies that ship next to the file (or next to the CLI itself).
///
/// <para>
/// Discovery rules:
/// <list type="bullet">
///   <item><description>Every <c>Kuestenlogik.Surgewave.*.dll</c> in the search directories is loaded
///   reflection-only-style and scanned for concrete public types implementing
///   <see cref="IValidatableConfig"/>.</description></item>
///   <item><description>Each candidate type must declare a <c>public const string SectionName</c>
///   field — that is the convention for binding configuration sections in this codebase. Types
///   without it are skipped silently (they're typically nested or test-only configs).</description></item>
///   <item><description>The matching JSON subtree is deserialised straight onto a fresh instance
///   via <see cref="JsonSerializer"/>, then <see cref="IValidatableConfig.Validate"/> is called.
///   Sections that aren't present in the file are skipped — only configured features get checked.</description></item>
/// </list>
/// </para>
///
/// <para>
/// We deliberately avoid <c>Microsoft.Extensions.Configuration</c> here: in .NET 10 the
/// <c>Microsoft.Extensions.Configuration.Json</c> NuGet is a thin shim that only resolves to a
/// real assembly when the consumer also pulls in the ASP.NET shared framework. Loading
/// <c>System.Text.Json</c> directly avoids dragging the full hosting stack into the CLI.
/// </para>
///
/// <para>
/// Exit codes: <c>0</c> when every present section validates clean, <c>1</c> when at least one
/// validator returns errors or the file cannot be loaded.
/// </para>
/// </summary>
internal sealed class ConfigValidateCommand : Command
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    public ConfigValidateCommand() : base("validate", "Validate a Surgewave appsettings.json against all known IValidatableConfig sections")
    {
        var pathArg = new Argument<string?>("path")
        {
            Description = "Path to the appsettings.json file (default: ./appsettings.json)",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var assembliesOption = new Option<string?>("--assemblies", "-a")
        {
            Description = "Directory to scan for Kuestenlogik.Surgewave.*.dll (default: directory of the config file, then CLI directory)",
        };
        var verboseOption = new Option<bool>("--verbose", "-v")
        {
            Description = "Print every section that was checked, not just the failing ones",
        };
        var outputOption = new Option<string>("--output", "-o")
        {
            Description = "Output format: text (default, human-readable) or json (machine-readable, one record per checked section)",
            DefaultValueFactory = _ => "text",
        };

        Arguments.Add(pathArg);
        Options.Add(assembliesOption);
        Options.Add(verboseOption);
        Options.Add(outputOption);

        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var path = parseResult.GetValue(pathArg) ?? "appsettings.json";
            var assembliesDir = parseResult.GetValue(assembliesOption);
            var verbose = parseResult.GetValue(verboseOption);
            var output = parseResult.GetValue(outputOption) ?? "text";
            return Task.FromResult(Execute(path, assembliesDir, verbose, output));
        });
    }

    /// <summary>
    /// Runs the same validation logic as the <c>surgewave config validate</c> CLI command,
    /// returning the same exit-code semantics (0 = all clean, 1 = at least one section
    /// failed or the file could not be loaded). Exposed so other commands (e.g.
    /// <c>surgewave plugin install --validate-config</c>) can chain validation onto their
    /// own workflows without re-implementing the merge / discover / validate pipeline.
    /// Defaults to text output; chained callers typically want text.
    /// </summary>
    internal static int Execute(string configPath, string? assembliesDir, bool verbose)
        => Execute(configPath, assembliesDir, verbose, output: "text");

    private static int Execute(string configPath, string? assembliesDir, bool verbose, string output)
    {
        var fullPath = Path.GetFullPath(configPath);
        if (!File.Exists(fullPath))
        {
            AnsiConsole.MarkupLine($"[red]Configuration file not found:[/] {fullPath}");
            return 1;
        }

        var pluginsDir = ConfigLoader.ResolvePluginsDirectory(fullPath, assembliesDir);
        var (rootNode, _) = ConfigLoader.LoadAndMerge(fullPath, pluginsDir);
        if (rootNode is null)
        {
            AnsiConsole.MarkupLine("[red]Top-level JSON value must be an object.[/]");
            return 1;
        }

        var searchDirs = BuildSearchDirs(fullPath, assembliesDir);
        var loaded = LoadSurgewaveAssemblies(searchDirs);
        if (loaded.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No Kuestenlogik.Surgewave.*.dll assemblies were found near the config file or the CLI.[/]");
            AnsiConsole.MarkupLine("[dim]Use --assemblies <dir> to point at a published Surgewave directory.[/]");
            return 1;
        }

        var configTypes = DiscoverValidatableConfigTypes(loaded);
        if (configTypes.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No IValidatableConfig types were discovered in the loaded assemblies.[/]");
            return 1;
        }

        var checks = new List<SectionCheck>();
        foreach (var type in configTypes)
        {
            var sectionName = ReadSectionName(type);
            if (sectionName is null) continue; // No SectionName convention — not bindable from a file.

            var sectionNode = ConfigLoader.NavigateSection(rootNode, sectionName);
            if (sectionNode is null) continue; // Not present in this file — nothing to validate.

            try
            {
                // Round-trip through the raw JSON text — JsonNode.Deserialize<T> can be brittle
                // when the source node was constructed manually, but ToJsonString() always works.
                var subTreeJson = sectionNode.ToJsonString();
                var instance = (IValidatableConfig?)JsonSerializer.Deserialize(subTreeJson, type, s_jsonOptions);
                if (instance is null)
                {
                    checks.Add(new SectionCheck(sectionName, type.Name, ["Section deserialised to null"]));
                    continue;
                }
                var errors = instance.Validate();
                checks.Add(new SectionCheck(sectionName, type.Name, errors));
            }
            catch (Exception ex)
            {
                checks.Add(new SectionCheck(sectionName, type.Name, [$"Bind/Validate threw: {ex.Message}"]));
            }
        }

        if (output.Equals("json", StringComparison.OrdinalIgnoreCase))
        {
            return RenderJson(checks);
        }
        return Render(fullPath, checks, verbose);
    }

    private static int RenderJson(IReadOnlyList<SectionCheck> checks)
    {
        // Stable, machine-readable per-section result. Caller can pipe through jq:
        //   surgewave config validate appsettings.json --output json | jq '.[] | select(.status=="fail")'
        var records = checks.Select(c => new
        {
            section = c.SectionName,
            @class = c.TypeName,
            status = c.Errors.Count == 0 ? "ok" : "fail",
            errors = c.Errors,
        }).ToArray();

        System.Console.Out.WriteLine(JsonSerializer.Serialize(records, s_jsonOutputOptions));
        return checks.Any(c => c.Errors.Count > 0) ? 1 : 0;
    }

    private static readonly JsonSerializerOptions s_jsonOutputOptions = new()
    {
        WriteIndented = true,
    };

    private static IReadOnlyList<string> BuildSearchDirs(string configFullPath, string? assembliesDir)
    {
        var dirs = new List<string>();
        if (!string.IsNullOrEmpty(assembliesDir))
        {
            dirs.Add(Path.GetFullPath(assembliesDir));
        }
        else
        {
            var configDir = Path.GetDirectoryName(configFullPath);
            if (!string.IsNullOrEmpty(configDir)) dirs.Add(configDir);
        }
        dirs.Add(AppContext.BaseDirectory);

        // Each Surgewave plugin lives under <base>/plugins/<plugin-id>/ — add those subdirs
        // so the discovery pass picks up plugin assemblies (and their IValidatableConfig
        // types) without needing the user to copy DLLs around. Mirrors how the broker
        // resolves plugin assemblies at startup.
        var withPluginDirs = new List<string>(dirs);
        foreach (var dir in dirs)
        {
            var pluginsRoot = Path.Combine(dir, "plugins");
            if (!Directory.Exists(pluginsRoot)) continue;
            foreach (var pluginDir in Directory.EnumerateDirectories(pluginsRoot))
            {
                withPluginDirs.Add(pluginDir);
            }
        }

        return withPluginDirs.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static IReadOnlyList<Assembly> LoadSurgewaveAssemblies(IReadOnlyList<string> searchDirs)
    {
        var loaded = new Dictionary<string, Assembly>(StringComparer.OrdinalIgnoreCase);

        // Seed with already-loaded assemblies (in-process discovery covers what the CLI shipped with).
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            var name = asm.GetName().Name;
            if (name is not null && name.StartsWith("Kuestenlogik.Surgewave.", StringComparison.Ordinal))
            {
                loaded.TryAdd(name, asm);
            }
        }

        // Install a resolver so transitive Surgewave dependencies (Broker → Connect → Streams → ...)
        // can be located in the same search dirs. Without this, AssemblyLoadContext.Default only
        // looks in the CLI's own bin and most loads fail with FileNotFoundException. The handler
        // is intentionally left attached for the rest of the CLI process — config validate is a
        // short-lived command, so leaking it does not matter in practice.
        AssemblyLoadContext.Default.Resolving += (ctx, name) =>
        {
            if (name.Name is null) return null;
            foreach (var dir in searchDirs)
            {
                var path = Path.Combine(dir, name.Name + ".dll");
                if (File.Exists(path))
                {
                    try { return ctx.LoadFromAssemblyPath(Path.GetFullPath(path)); }
                    catch { /* try next dir */ }
                }
            }
            return null;
        };

        // Use the Default AssemblyLoadContext (rather than Assembly.LoadFrom) so that loading
        // a DLL whose SimpleName already exists in the process resolves to the existing
        // instance. Without this, Kuestenlogik.Surgewave.Core.dll would get loaded twice (once from the CLI's
        // bin, once from the Broker's bin), and IValidatableConfig from the second load would
        // not be assignable from the CLI's IValidatableConfig — every type check would fail.
        //
        // Two filename patterns are matched: "Kuestenlogik.Surgewave.*.dll" for the library assemblies and
        // "surgewave-*.dll" for the service executables (surgewave-broker, surgewave-marketplace, ...) which
        // override AssemblyName but still contain types in the Kuestenlogik.Surgewave.* namespace.
        var dllPatterns = new[] { "Kuestenlogik.Surgewave.*.dll", "surgewave-*.dll" };
        foreach (var dir in searchDirs)
        {
            if (!Directory.Exists(dir)) continue;
            foreach (var pattern in dllPatterns)
            {
                foreach (var dll in Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly))
                {
                    var simpleName = Path.GetFileNameWithoutExtension(dll);
                    if (loaded.ContainsKey(simpleName)) continue;
                    try
                    {
                        var asm = AssemblyLoadContext.Default.LoadFromAssemblyPath(Path.GetFullPath(dll));
                        loaded[asm.GetName().Name ?? simpleName] = asm;
                    }
                    catch
                    {
                        // Skip unloadable DLLs (missing transitive deps, native bits, etc.). The
                        // discovery pass below tolerates ReflectionTypeLoadException too, so a
                        // partial load is still useful.
                    }
                }
            }
        }

        return loaded.Values.ToList();
    }

    private static IReadOnlyList<Type> DiscoverValidatableConfigTypes(IReadOnlyList<Assembly> assemblies)
    {
        var result = new List<Type>();
        foreach (var asm in assemblies)
        {
            Type?[] types;
            try
            {
                types = asm.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types;
            }
            catch
            {
                continue;
            }

            foreach (var type in types)
            {
                if (type is null) continue;
                if (type.IsAbstract || type.IsInterface) continue;
                if (!typeof(IValidatableConfig).IsAssignableFrom(type)) continue;
                if (type.GetConstructor(Type.EmptyTypes) is null) continue;
                result.Add(type);
            }
        }
        return result;
    }

    private static string? ReadSectionName(Type type)
    {
        var field = type.GetField("SectionName", BindingFlags.Public | BindingFlags.Static);
        if (field is null || field.FieldType != typeof(string)) return null;
        return field.GetRawConstantValue() as string ?? field.GetValue(null) as string;
    }

    private static int Render(string configPath, IReadOnlyList<SectionCheck> checks, bool verbose)
    {
        AnsiConsole.MarkupLine($"[bold]Validating:[/] {Markup.Escape(configPath)}");
        AnsiConsole.WriteLine();

        if (checks.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No validatable sections were found in this file.[/]");
            return 0;
        }

        var failing = checks.Where(c => c.Errors.Count > 0).ToList();
        var passing = checks.Where(c => c.Errors.Count == 0).ToList();

        if (verbose && passing.Count > 0)
        {
            var okTable = new Table()
                .AddColumn("Section")
                .AddColumn("Class")
                .AddColumn("Status");
            foreach (var ok in passing)
            {
                okTable.AddRow(Markup.Escape(ok.SectionName), Markup.Escape(ok.TypeName), "[green]ok[/]");
            }
            AnsiConsole.Write(okTable);
            AnsiConsole.WriteLine();
        }

        if (failing.Count == 0)
        {
            AnsiConsole.MarkupLine($"[green]All {checks.Count} configured section(s) are valid.[/]");
            return 0;
        }

        AnsiConsole.MarkupLine($"[red]{failing.Count} section(s) failed validation:[/]");
        AnsiConsole.WriteLine();
        foreach (var fail in failing)
        {
            AnsiConsole.MarkupLine($"[red]x[/] [bold]{Markup.Escape(fail.SectionName)}[/] ({Markup.Escape(fail.TypeName)})");
            foreach (var error in fail.Errors)
            {
                AnsiConsole.MarkupLine($"    [red]-[/] {Markup.Escape(error)}");
            }
        }
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[dim]{passing.Count} section(s) passed, {failing.Count} failed.[/]");
        return 1;
    }

    private sealed record SectionCheck(string SectionName, string TypeName, IReadOnlyList<string> Errors);
}
