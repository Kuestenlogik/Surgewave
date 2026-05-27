using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Show dependency tree for a plugin (surgewave plugins deps)
/// </summary>
public class DepsCommand : CommandBase
{
    private readonly Argument<string> _packageIdArg = new("package-id")
    {
        Description = "Package ID to show dependencies for"
    };

    private readonly Option<string> _installDirOpt = new("--install-dir")
    {
        Description = "Connector installation directory",
        DefaultValueFactory = _ => GetDefaultInstallDirectory()
    };

    private readonly Option<bool> _flatOpt = new("--flat")
    {
        Description = "Show flat list instead of tree"
    };

    public DepsCommand() : base("deps", "Show dependency tree for a plugin")
    {
        Arguments.Add(_packageIdArg);
        Options.Add(_installDirOpt);
        Options.Add(_flatOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var packageId = parseResult.GetValue(_packageIdArg);
        var installDir = parseResult.GetValue(_installDirOpt) ?? GetDefaultInstallDirectory();
        var flat = parseResult.GetValue(_flatOpt);

        if (string.IsNullOrEmpty(packageId))
        {
            WriteError("Package ID is required.");
            return 1;
        }

        // Load repository configuration
        var config = RepositoryConfiguration.Load();

        using var repoManager = new ConnectorRepositoryManager(installDir);

        // Add configured repositories
        foreach (var repo in config.CreateRepositories())
        {
            if (repo.Name != "nuget.org")
            {
                repoManager.AddRepository(repo);
            }
        }

        // Get dependency tree
        var tree = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Resolving dependencies...", async _ =>
                await repoManager.GetDependencyTreeAsync(packageId, ct));

        if (tree == null)
        {
            WriteError($"Package not found: {packageId}");
            return 1;
        }

        if (flat)
        {
            // Flat list
            var allDeps = tree.Flatten().ToList();

            var table = new Table();
            table.Border(TableBorder.Rounded);
            table.AddColumn("Package");
            table.AddColumn("Version");
            table.AddColumn("Status");

            foreach (var node in allDeps)
            {
                var status = GetStatusMarkup(node);
                table.AddRow(node.PackageId, node.Version, status);
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"\n[dim]Total: {allDeps.Count} package(s)[/]");
        }
        else
        {
            // Tree view using Spectre.Console Tree
            var spectreTree = new Tree($"[bold]{tree.PackageId}[/]@{tree.Version} {GetStatusMarkup(tree)}");

            AddChildNodes(spectreTree, tree.Children);

            AnsiConsole.Write(spectreTree);

            // Summary
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[dim]Total dependencies: {tree.TotalDependencyCount}[/]");
            AnsiConsole.MarkupLine($"[dim]Max depth: {tree.MaxDepth}[/]");

            var missing = tree.GetMissingDependencies().ToList();
            if (missing.Count > 0)
            {
                AnsiConsole.MarkupLine($"[yellow]Missing: {missing.Count} package(s)[/]");
            }

            var uninstalled = tree.GetUninstalledDependencies().ToList();
            if (uninstalled.Count > 0)
            {
                AnsiConsole.MarkupLine($"[cyan]Not installed: {uninstalled.Count} package(s)[/]");
            }
        }

        return 0;
    }

    private static void AddChildNodes(IHasTreeNodes parent, IReadOnlyList<DependencyTreeNode> children)
    {
        foreach (var child in children)
        {
            var label = $"{child.PackageId}@{child.Version} {GetStatusMarkup(child)}";
            var node = parent.AddNode(label);

            if (child.Children.Count > 0 && !child.IsCircular)
            {
                AddChildNodes(node, child.Children);
            }
        }
    }

    private static string GetStatusMarkup(DependencyTreeNode node)
    {
        if (node.IsCircular)
            return "[red](circular)[/]";
        if (node.IsMissing)
            return node.IsOptional ? "[yellow](missing, optional)[/]" : "[red](missing)[/]";
        if (!node.IsInstalled)
            return "[cyan](not installed)[/]";
        if (node.InstalledVersion != null && node.InstalledVersion != node.Version)
            return $"[yellow](installed: {node.InstalledVersion})[/]";
        return "[green](installed)[/]";
    }

    private static string GetDefaultInstallDirectory()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".surgewave", "connectors");
    }
}
