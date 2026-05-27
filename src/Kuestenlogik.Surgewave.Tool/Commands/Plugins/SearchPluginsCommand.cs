using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Plugins.Sources;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Search for plugins in configured repositories (surgewave plugins search)
/// </summary>
public class SearchPluginsCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Argument<string?> _queryArg = new("query")
    {
        Description = "Search query (name, tags, description)",
        Arity = ArgumentArity.ZeroOrOne
    };

    private readonly Option<string?> _repositoryOpt = new("--repository", "-r")
    {
        Description = "Search only in specific repository"
    };

    private readonly Option<int> _takeOpt = new("--take", "-t")
    {
        Description = "Number of results to return",
        DefaultValueFactory = _ => 20
    };

    private readonly Option<string> _installDirOpt = new("--install-dir")
    {
        Description = "Connector installation directory",
        DefaultValueFactory = _ => GetDefaultInstallDirectory()
    };

    public SearchPluginsCommand() : base("search", "Search for plugins in configured repositories")
    {
        Arguments.Add(_queryArg);
        Options.Add(_repositoryOpt);
        Options.Add(_takeOpt);
        Options.Add(_installDirOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var query = parseResult.GetValue(_queryArg);
        var repositoryName = parseResult.GetValue(_repositoryOpt);
        var take = parseResult.GetValue(_takeOpt);
        var installDir = parseResult.GetValue(_installDirOpt) ?? GetDefaultInstallDirectory();
        var format = GetFormat(parseResult);

        // Load repository configuration
        var config = RepositoryConfiguration.Load();

        using var repoManager = new ConnectorRepositoryManager(installDir);

        // Add configured repositories (skip default nuget.org as it's already added)
        foreach (var repo in config.CreateRepositories())
        {
            if (repo.Name != "nuget.org")
            {
                repoManager.AddRepository(repo);
            }
        }

        // Search
        IReadOnlyList<ConnectorPackageInfo> results;

        if (!string.IsNullOrWhiteSpace(repositoryName))
        {
            // Search specific repository
            var repo = repoManager.Repositories.FirstOrDefault(r =>
                r.Name.Equals(repositoryName, StringComparison.OrdinalIgnoreCase));

            if (repo == null)
            {
                WriteError($"Repository not found: {repositoryName}");
                WriteMarkup($"[dim]Available: {string.Join(", ", repoManager.Repositories.Select(r => r.Name))}[/]");
                return 1;
            }

            results = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync($"Searching {repo.Name}...", async _ =>
                    await repo.SearchAsync(query, 0, take, ct));
        }
        else
        {
            // Search all repositories
            results = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync("Searching repositories...", async _ =>
                    await repoManager.SearchAsync(query, 0, take, ct));
        }

        // Also search configured plugin sources
        var sourceResults = await SearchPluginSourcesAsync(query, ct);

        if (results.Count == 0 && sourceResults.Count == 0)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                WriteWarning("No plugins found in configured repositories or sources.");
            }
            else
            {
                WriteWarning($"No plugins found matching '{query}'.");
            }
            return 0;
        }

        // Output repository results
        if (results.Count > 0)
        {
            if (format == OutputFormat.Json)
            {
                OutputJson(results);
            }
            else if (format == OutputFormat.Plain)
            {
                OutputPlain(results);
            }
            else
            {
                OutputTable(results);
            }
        }

        // Output plugin source results
        if (sourceResults.Count > 0)
        {
            if (format == OutputFormat.Json)
            {
                OutputSourceJson(sourceResults);
            }
            else if (format == OutputFormat.Plain)
            {
                OutputSourcePlain(sourceResults);
            }
            else
            {
                OutputSourceTable(sourceResults);
            }
        }

        return 0;
    }

    private static async Task<IReadOnlyList<PluginPackageInfo>> SearchPluginSourcesAsync(
        string? query, CancellationToken ct)
    {
        var config = PluginSourceConfig.Load();
        if (config.Sources.Count == 0)
            return [];

        var allResults = new List<PluginPackageInfo>();

        foreach (var source in PluginSourceFactory.CreateAll(config))
        {
            try
            {
                var results = await source.SearchAsync(query, ct);
                allResults.AddRange(results);
            }
            catch
            {
                // Skip sources that fail silently
            }
            finally
            {
                if (source is IDisposable disposable)
                    disposable.Dispose();
            }
        }

        return allResults;
    }

    private static void OutputSourceJson(IReadOnlyList<PluginPackageInfo> results)
    {
        var json = JsonSerializer.Serialize(results.Select(p => new
        {
            p.Id,
            p.Version,
            p.Description,
            p.Authors,
            p.Source
        }), JsonOptions);

        System.Console.WriteLine(json);
    }

    private static void OutputSourcePlain(IReadOnlyList<PluginPackageInfo> results)
    {
        foreach (var package in results)
        {
            System.Console.WriteLine($"{package.Id}\t{package.Version}\t{package.Source}");
        }
    }

    private static void OutputSourceTable(IReadOnlyList<PluginPackageInfo> results)
    {
        var table = new Table();
        table.AddColumn("Plugin");
        table.AddColumn("Version");
        table.AddColumn("Source");
        table.AddColumn("Description");

        foreach (var package in results)
        {
            var description = package.Description ?? "";
            if (description.Length > 60)
                description = description[..57] + "...";

            table.AddRow(
                $"[bold]{package.Id}[/]",
                package.Version,
                $"[dim]{package.Source}[/]",
                $"[dim]{Markup.Escape(description)}[/]");
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"\n[dim]Found {results.Count} plugin(s) from sources[/]");
        AnsiConsole.MarkupLine("[dim]Install with: surgewave plugins install <plugin-id> --source <source-name>[/]");
    }

    private static void OutputJson(IReadOnlyList<ConnectorPackageInfo> results)
    {
        var json = JsonSerializer.Serialize(results.Select(p => new
        {
            p.PackageId,
            p.Name,
            p.Version,
            p.Description,
            p.Author,
            p.DownloadCount,
            p.IsInstalled,
            p.InstalledVersion,
            p.ConnectorTypes,
            p.Tags
        }), JsonOptions);

        System.Console.WriteLine(json);
    }

    private static void OutputPlain(IReadOnlyList<ConnectorPackageInfo> results)
    {
        foreach (var package in results)
        {
            var installed = package.IsInstalled ? $" (installed: {package.InstalledVersion})" : "";
            System.Console.WriteLine($"{package.PackageId}\t{package.Version}{installed}");
        }
    }

    private static void OutputTable(IReadOnlyList<ConnectorPackageInfo> results)
    {
        var table = new Table();
        table.AddColumn("Package");
        table.AddColumn("Version");
        table.AddColumn("Type");
        table.AddColumn("Downloads");
        table.AddColumn("Status");

        foreach (var package in results)
        {
            var types = string.Join("/", package.ConnectorTypes.Select(t =>
                t == "source" ? "[cyan]source[/]" : "[green]sink[/]"));

            if (string.IsNullOrEmpty(types))
            {
                types = "[dim]unknown[/]";
            }

            var downloads = FormatDownloads(package.DownloadCount);

            string status;
            if (package.IsInstalled)
            {
                if (package.InstalledVersion == package.Version)
                {
                    status = "[green]installed[/]";
                }
                else
                {
                    status = $"[yellow]update available ({package.InstalledVersion})[/]";
                }
            }
            else
            {
                status = "[dim]not installed[/]";
            }

            table.AddRow(
                $"[bold]{package.Name}[/]\n[dim]{package.PackageId}[/]",
                package.Version,
                types,
                downloads,
                status);
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"\n[dim]Found {results.Count} connector(s)[/]");

        // Show install hint
        AnsiConsole.MarkupLine("\n[dim]Install with: surgewave plugins install <package-id> --from-nuget[/]");
    }

    private static string FormatDownloads(long count)
    {
        return count switch
        {
            >= 1_000_000 => $"{count / 1_000_000.0:F1}M",
            >= 1_000 => $"{count / 1_000.0:F1}K",
            _ => count.ToString()
        };
    }

    private static string GetDefaultInstallDirectory()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".surgewave", "connectors");
    }
}
