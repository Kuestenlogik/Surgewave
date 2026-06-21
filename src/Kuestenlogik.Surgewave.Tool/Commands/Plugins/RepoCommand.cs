using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// `surgewave plugins repo …` — manages the broker's canonical repository
/// list at <c>/api/plugins/repositories</c>. Edits land in the broker's
/// <c>surgewave-repositories.json</c> in its DataDirectory; the broker's
/// own SearchPlugins handler re-syncs on every search via the live-resync
/// added in commit fcac93a, so additions take effect immediately on the
/// next browse without a broker restart.
///
/// Replaces the previous <see cref="RepositoryConfiguration"/>-Load/Save
/// flow that wrote to a CLI-local <c>~/.surgewave/surgewave-repositories.json</c>
/// nothing else read. Backwards-compat shim for the old file is intentionally
/// not provided — there's no use-case where the old file should win over the
/// broker's truth.
/// </summary>
public class RepoCommand : CommandBase
{
    public RepoCommand() : base("repo", "Manage the broker's plugin repositories")
    {
        Subcommands.Add(new RepoListCommand());
        Subcommands.Add(new RepoAddCommand());
        Subcommands.Add(new RepoRemoveCommand());
    }
}

internal static class RepoClientFactory
{
    public static BrokerRepositoryClient Create(ParseResult parseResult)
    {
        var url = parseResult.GetValue(GlobalOptions.BrokerUrl) ?? "https://localhost:9093";
        return new BrokerRepositoryClient(url);
    }
}

public class RepoListCommand : CommandBase
{
    public RepoListCommand() : base("list", "List repositories the broker is searching")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        using var client = RepoClientFactory.Create(parseResult);
        IReadOnlyList<RepositoryEntry> repos;
        try
        {
            repos = await client.ListAsync(ct);
        }
        catch (BrokerUnreachableException ex)
        {
            WriteError(ex.Message);
            return 1;
        }

        if (repos.Count == 0)
        {
            WriteWarning("No repositories configured on the broker.");
            return 0;
        }

        var table = new Table();
        table.AddColumn("Name");
        table.AddColumn("Type");
        table.AddColumn("Source");
        table.AddColumn("Prefix");
        table.AddColumn("Status");

        foreach (var repo in repos)
        {
            var status = repo.Enabled ? "[green]enabled[/]" : "[dim]disabled[/]";
            table.AddRow(
                $"[bold]{repo.Name}[/]",
                repo.Type.ToString(),
                $"[dim]{repo.Source}[/]",
                string.IsNullOrEmpty(repo.PackagePrefix) ? "[dim]—[/]" : $"[dim]{repo.PackagePrefix}[/]",
                status);
        }
        AnsiConsole.Write(table);
        return 0;
    }
}

public class RepoAddCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name")
    {
        Description = "Repository name"
    };

    private readonly Argument<string> _sourceArg = new("source")
    {
        Description = "Repository source URL"
    };

    private readonly Option<string> _typeOpt = new("--type", "-t")
    {
        Description = "Repository type (NuGet, Http, Marketplace)",
        DefaultValueFactory = _ => "NuGet"
    };

    private readonly Option<string?> _prefixOpt = new("--prefix", "-p")
    {
        Description = "Package ID prefix filter"
    };

    public RepoAddCommand() : base("add", "Add a repository to the broker")
    {
        Arguments.Add(_nameArg);
        Arguments.Add(_sourceArg);
        Options.Add(_typeOpt);
        Options.Add(_prefixOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);
        var source = parseResult.GetValue(_sourceArg);
        var typeStr = parseResult.GetValue(_typeOpt) ?? "NuGet";
        var prefix = parseResult.GetValue(_prefixOpt);

        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(source))
        {
            WriteError("Name and source are required.");
            return 1;
        }
        if (!Enum.TryParse<RepositoryType>(typeStr, ignoreCase: true, out var type))
        {
            WriteError($"Invalid repository type: {typeStr}. Valid: NuGet, Http, Marketplace");
            return 1;
        }

        var entry = new RepositoryEntry
        {
            Name = name,
            Type = type,
            Source = source,
            PackagePrefix = prefix,
            Enabled = true,
        };

        using var client = RepoClientFactory.Create(parseResult);
        try
        {
            var saved = await client.AddAsync(entry, ct);
            WriteSuccess($"Added repository '{saved.Name}' on the broker.");
            return 0;
        }
        catch (BrokerUnreachableException ex) { WriteError(ex.Message); return 1; }
        catch (InvalidOperationException ex) { WriteError(ex.Message); return 1; }
    }
}

public class RepoRemoveCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name")
    {
        Description = "Repository name to remove"
    };

    public RepoRemoveCommand() : base("remove", "Remove a repository from the broker")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);
        if (string.IsNullOrEmpty(name))
        {
            WriteError("Repository name is required.");
            return 1;
        }
        if (!ConfirmDestructive(parseResult, $"Remove repository '[cyan]{name}[/]' from the broker?"))
        {
            WriteWarning("Remove cancelled.");
            return 0;
        }

        using var client = RepoClientFactory.Create(parseResult);
        try
        {
            await client.RemoveAsync(name, ct);
            WriteSuccess($"Removed repository '{name}'.");
            return 0;
        }
        catch (BrokerUnreachableException ex) { WriteError(ex.Message); return 1; }
        catch (RepositoryNotFoundException) { WriteError($"Repository not found: {name}"); return 1; }
        catch (InvalidOperationException ex) { WriteError(ex.Message); return 1; }
    }
}
