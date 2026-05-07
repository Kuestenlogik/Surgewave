using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Sources;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Command group for managing plugin sources (surgewave plugins source ...)
/// </summary>
public class SourceCommand : CommandBase
{
    public SourceCommand() : base("source", "Manage plugin sources")
    {
        Subcommands.Add(new SourceListCommand());
        Subcommands.Add(new SourceAddCommand());
        Subcommands.Add(new SourceRemoveCommand());
    }
}

/// <summary>
/// List configured plugin sources (surgewave plugins source list)
/// </summary>
public class SourceListCommand : CommandBase
{
    public SourceListCommand() : base("list", "List configured plugin sources")
    {
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var config = PluginSourceConfig.Load();

        if (config.Sources.Count == 0)
        {
            WriteWarning("No plugin sources configured.");
            WriteMarkup("[dim]Add a source with: surgewave plugins source add <name> <url> --type <nuget|http|github>[/]");
            return Task.FromResult(0);
        }

        var table = new Table();
        table.AddColumn("Name");
        table.AddColumn("Type");
        table.AddColumn("URL");

        foreach (var source in config.Sources)
        {
            table.AddRow(
                $"[bold]{source.Name}[/]",
                source.Type,
                $"[dim]{source.Url}[/]");
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"\n[dim]{config.Sources.Count} source(s) configured[/]");

        return Task.FromResult(0);
    }
}

/// <summary>
/// Add a new plugin source (surgewave plugins source add)
/// </summary>
public class SourceAddCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name")
    {
        Description = "Source name"
    };

    private readonly Argument<string> _urlArg = new("url")
    {
        Description = "Source URL (NuGet feed URL, marketplace URL, or GitHub owner/repo)"
    };

    private readonly Option<string> _typeOpt = new("--type", "-t")
    {
        Description = "Source type: nuget, http, or github",
        DefaultValueFactory = _ => "nuget"
    };

    private readonly Option<string?> _apiKeyOpt = new("--api-key")
    {
        Description = "API key for authenticated feeds"
    };

    public SourceAddCommand() : base("add", "Add a new plugin source")
    {
        Arguments.Add(_nameArg);
        Arguments.Add(_urlArg);
        Options.Add(_typeOpt);
        Options.Add(_apiKeyOpt);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);
        var url = parseResult.GetValue(_urlArg);
        var type = parseResult.GetValue(_typeOpt) ?? "nuget";
        var apiKey = parseResult.GetValue(_apiKeyOpt);

        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(url))
        {
            WriteError("Name and URL are required.");
            return Task.FromResult(1);
        }

        // Validate type
        var validTypes = new[] { "nuget", "http", "github" };
        if (!validTypes.Contains(type, StringComparer.OrdinalIgnoreCase))
        {
            WriteError($"Invalid source type: '{type}'. Valid types: {string.Join(", ", validTypes)}");
            return Task.FromResult(1);
        }

        var config = PluginSourceConfig.Load();

        // Check for duplicate names
        if (config.Sources.Any(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
        {
            WriteError($"Source '{name}' already exists. Remove it first with: surgewave plugins source remove {name}");
            return Task.FromResult(1);
        }

        config.Sources.Add(new PluginSourceConfig.SourceEntry
        {
            Name = name,
            Type = type.ToLowerInvariant(),
            Url = url,
            ApiKey = apiKey
        });

        config.Save();
        WriteSuccess($"Added plugin source '{name}' ({type}: {url})");

        return Task.FromResult(0);
    }
}

/// <summary>
/// Remove a plugin source (surgewave plugins source remove)
/// </summary>
public class SourceRemoveCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name")
    {
        Description = "Source name to remove"
    };

    public SourceRemoveCommand() : base("remove", "Remove a plugin source")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg);

        if (string.IsNullOrEmpty(name))
        {
            WriteError("Source name is required.");
            return Task.FromResult(1);
        }

        var config = PluginSourceConfig.Load();
        var removed = config.Sources.RemoveAll(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (removed == 0)
        {
            WriteError($"Source not found: '{name}'");
            var available = string.Join(", ", config.Sources.Select(s => s.Name));
            if (!string.IsNullOrEmpty(available))
            {
                WriteMarkup($"[dim]Available sources: {available}[/]");
            }
            return Task.FromResult(1);
        }

        config.Save();
        WriteSuccess($"Removed plugin source '{name}'");

        return Task.FromResult(0);
    }
}
