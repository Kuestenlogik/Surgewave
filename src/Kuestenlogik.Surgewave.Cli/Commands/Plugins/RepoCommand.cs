using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Command group for managing plugin repositories (surgewave plugins repo ...)
/// </summary>
public class RepoCommand : CommandBase
{
    public RepoCommand() : base("repo", "Manage plugin repositories")
    {
        Subcommands.Add(new RepoListCommand());
        Subcommands.Add(new RepoAddCommand());
        Subcommands.Add(new RepoRemoveCommand());
    }
}

/// <summary>
/// List configured repositories (surgewave plugins repo list)
/// </summary>
public class RepoListCommand : CommandBase
{
    public RepoListCommand() : base("list", "List configured repositories")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var config = RepositoryConfiguration.Load();

        if (config.Repositories.Count == 0)
        {
            WriteWarning("No repositories configured.");
            return Task.FromResult(0);
        }

        var table = new Table();
        table.AddColumn("Name");
        table.AddColumn("Type");
        table.AddColumn("Source");
        table.AddColumn("Status");

        foreach (var repo in config.Repositories)
        {
            var status = repo.Enabled ? "[green]enabled[/]" : "[dim]disabled[/]";
            var isDefault = repo.Name == config.DefaultRepository ? " [yellow](default)[/]" : "";

            table.AddRow(
                $"{repo.Name}{isDefault}",
                repo.Type.ToString(),
                $"[dim]{repo.Source}[/]",
                status);
        }

        AnsiConsole.Write(table);
        return Task.FromResult(0);
    }
}

/// <summary>
/// Add a new repository (surgewave plugins repo add)
/// </summary>
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
        Description = "Repository type (nuget, http)",
        DefaultValueFactory = _ => "nuget"
    };

    private readonly Option<string?> _prefixOpt = new("--prefix", "-p")
    {
        Description = "Package ID prefix filter"
    };

    private readonly Option<bool> _defaultOpt = new("--default")
    {
        Description = "Set as default repository"
    };

    public RepoAddCommand() : base("add", "Add a new repository")
    {
        Arguments.Add(_nameArg);
        Arguments.Add(_sourceArg);
        Options.Add(_typeOpt);
        Options.Add(_prefixOpt);
        Options.Add(_defaultOpt);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);
        var source = parseResult.GetValue(_sourceArg);
        var typeStr = parseResult.GetValue(_typeOpt) ?? "nuget";
        var prefix = parseResult.GetValue(_prefixOpt);
        var setDefault = parseResult.GetValue(_defaultOpt);

        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(source))
        {
            WriteError("Name and source are required.");
            return Task.FromResult(1);
        }

        if (!Enum.TryParse<RepositoryType>(typeStr, ignoreCase: true, out var type))
        {
            WriteError($"Invalid repository type: {typeStr}. Valid types: nuget, http");
            return Task.FromResult(1);
        }

        var config = RepositoryConfiguration.Load();

        config.AddRepository(new RepositoryEntry
        {
            Name = name,
            Type = type,
            Source = source,
            PackagePrefix = prefix,
            Enabled = true
        });

        if (setDefault)
        {
            config.DefaultRepository = name;
        }

        config.Save();
        WriteSuccess($"Added repository '{name}'");

        if (setDefault)
        {
            WriteMarkup("[dim]Set as default repository[/]");
        }

        return Task.FromResult(0);
    }
}

/// <summary>
/// Remove a repository (surgewave plugins repo remove)
/// </summary>
public class RepoRemoveCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name")
    {
        Description = "Repository name to remove"
    };

    public RepoRemoveCommand() : base("remove", "Remove a repository")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);

        if (string.IsNullOrEmpty(name))
        {
            WriteError("Repository name is required.");
            return Task.FromResult(1);
        }

        if (!ConfirmDestructive(parseResult, $"Remove repository '[cyan]{name}[/]'?"))
        {
            WriteWarning("Remove cancelled.");
            return Task.FromResult(0);
        }

        var config = RepositoryConfiguration.Load();

        if (!config.RemoveRepository(name))
        {
            WriteError($"Repository not found: {name}");
            return Task.FromResult(1);
        }

        // Clear default if we removed it
        if (config.DefaultRepository == name)
        {
            config.DefaultRepository = config.Repositories.FirstOrDefault()?.Name;
        }

        config.Save();
        WriteSuccess($"Removed repository '{name}'");

        return Task.FromResult(0);
    }
}
