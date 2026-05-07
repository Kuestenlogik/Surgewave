using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Config;

/// <summary>
/// Command for managing Surgewave CLI configuration (surgewave config)
/// </summary>
public class ConfigCommand : Command
{
    public ConfigCommand() : base("config", "Manage Surgewave CLI configuration")
    {
        Subcommands.Add(new ConfigShowCommand());
        Subcommands.Add(new ConfigInitCommand());
        Subcommands.Add(new ConfigSetCommand());
        Subcommands.Add(new ConfigGetCommand());
        Subcommands.Add(new ConfigProfileCommand());
        Subcommands.Add(new ConfigValidateCommand());
        Subcommands.Add(new ConfigViewCommand());
    }
}

/// <summary>
/// Shows current configuration (surgewave config show)
/// </summary>
internal sealed class ConfigShowCommand : Command
{
    public ConfigShowCommand() : base("show", "Show current configuration")
    {
        this.SetAction((ParseResult _, CancellationToken _) =>
        {
            Execute();
            return Task.FromResult(0);
        });
    }

    private static void Execute()
    {
        var configPath = SurgewaveConfig.ConfigFilePath;

        if (!File.Exists(configPath))
        {
            AnsiConsole.MarkupLine($"[yellow]No configuration file found.[/]");
            AnsiConsole.MarkupLine($"[dim]Run 'surgewave config init' to create one at {configPath}[/]");
            return;
        }

        var config = SurgewaveConfig.Load();
        var effective = config.GetEffective();

        AnsiConsole.MarkupLine($"[bold]Configuration file:[/] {configPath}");
        AnsiConsole.WriteLine();

        var table = new Table()
            .AddColumn("Setting")
            .AddColumn("Value")
            .AddColumn(new TableColumn("Source").Centered());

        AddRow(table, "broker", effective.BootstrapServer, config.BootstrapServer != effective.BootstrapServer ? "profile" : "config");
        AddRow(table, "format", effective.Format, config.Format != effective.Format ? "profile" : "config");
        AddRow(table, "verbose", effective.Verbose?.ToString()?.ToLowerInvariant(), config.Verbose != effective.Verbose ? "profile" : "config");
        AddRow(table, "timeout", effective.Timeout?.ToString(), config.Timeout != effective.Timeout ? "profile" : "config");
        AddRow(table, "profile", config.ActiveProfile, "config");

        AnsiConsole.Write(table);

        if (config.Profiles != null && config.Profiles.Count > 0)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[bold]Available profiles:[/]");
            foreach (var (name, profile) in config.Profiles)
            {
                var active = name == config.ActiveProfile ? " [green](active)[/]" : "";
                AnsiConsole.MarkupLine($"  [cyan]{name}[/]{active}: {profile.BootstrapServer ?? "default"}");
            }
        }
    }

    private static void AddRow(Table table, string setting, string? value, string source)
    {
        if (value != null)
        {
            table.AddRow(setting, value, $"[dim]{source}[/]");
        }
    }
}

/// <summary>
/// Initializes a new config file (surgewave config init)
/// </summary>
internal sealed class ConfigInitCommand : Command
{
    private readonly Option<bool> _forceOpt = new("--force", "-f") { Description = "Overwrite existing config file", DefaultValueFactory = _ => false };
    private readonly Option<string?> _pluginOpt = new("--plugin", "-p")
    {
        Description = "Generate a ready-to-paste appsettings.json section for an installed plugin (with Enabled=true already set)"
    };
    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory to look up the plugin",
        DefaultValueFactory = _ => "plugins"
    };

    public ConfigInitCommand() : base("init", "Initialize a CLI config file, or generate a plugin config section (--plugin)")
    {
        Options.Add(_forceOpt);
        Options.Add(_pluginOpt);
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var pluginId = parseResult.GetValue(_pluginOpt);

        if (!string.IsNullOrWhiteSpace(pluginId))
        {
            return await GeneratePluginConfigAsync(pluginId, parseResult.GetValue(_directoryOpt) ?? "plugins", ct);
        }

        // Default: initialize CLI config
        var force = parseResult.GetValue(_forceOpt);
        var configPath = SurgewaveConfig.ConfigFilePath;

        if (File.Exists(configPath) && !force)
        {
            AnsiConsole.MarkupLine($"[yellow]Configuration file already exists at {configPath}[/]");
            AnsiConsole.MarkupLine("[dim]Use --force to overwrite[/]");
            return 0;
        }

        SurgewaveConfig.InitializeIfNotExists();

        if (force && File.Exists(configPath))
        {
            File.Delete(configPath);
            SurgewaveConfig.InitializeIfNotExists();
        }

        AnsiConsole.MarkupLine($"[green]Created configuration file at {configPath}[/]");
        return 0;
    }

    private static async Task<int> GeneratePluginConfigAsync(string pluginId, string directory, CancellationToken ct)
    {
        var pluginsDir = Path.GetFullPath(directory);
        if (!Directory.Exists(pluginsDir))
        {
            AnsiConsole.MarkupLine($"[red]Plugins directory does not exist:[/] {pluginsDir}");
            return 1;
        }

        var manager = new Kuestenlogik.Surgewave.Plugins.Packaging.PluginPackageManager();
        Kuestenlogik.Surgewave.Plugins.Packaging.InstalledPlugin? match = null;
        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            if (string.Equals(plugin.Id, pluginId, StringComparison.OrdinalIgnoreCase))
            {
                match = plugin;
                break;
            }
        }

        if (match is null)
        {
            AnsiConsole.MarkupLine($"[red]Plugin '{pluginId}' not installed in {pluginsDir}[/]");
            return 1;
        }

        var manifest = match.Manifest!;
        var settingsFileName = string.IsNullOrWhiteSpace(manifest.PluginSettings)
            ? "pluginsettings.json"
            : manifest.PluginSettings;
        var settingsPath = Path.Combine(match.InstallPath!, settingsFileName);

        if (!File.Exists(settingsPath))
        {
            AnsiConsole.MarkupLine($"[yellow]Plugin '{pluginId}' does not bundle a {settingsFileName}.[/]");
            AnsiConsole.MarkupLine("[dim]Check the plugin docs for the required config section.[/]");
            return 1;
        }

        // Read the defaults and flip Enabled to true — the whole point of this command is
        // to give the operator a copy-paste-ready section that activates the plugin.
        var jsonText = await File.ReadAllTextAsync(settingsPath, ct);
        var node = System.Text.Json.Nodes.JsonNode.Parse(jsonText, documentOptions: new System.Text.Json.JsonDocumentOptions
        {
            CommentHandling = System.Text.Json.JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
        });
        if (node is System.Text.Json.Nodes.JsonObject root)
        {
            FlipEnabledToTrue(root);
        }

        var output = node?.ToJsonString(new System.Text.Json.JsonSerializerOptions { WriteIndented = true }) ?? jsonText;

        // Write to stdout so the operator can pipe it directly:
        //   surgewave config init --plugin mqtt >> appsettings.json
        System.Console.Out.WriteLine(output);
        return 0;
    }

    /// <summary>
    /// Recursively walks the JSON tree and sets any "Enabled" property to true.
    /// This makes the output paste-ready — the operator does not need to manually
    /// flip the flag after copying the section into their appsettings.json.
    /// </summary>
    private static void FlipEnabledToTrue(System.Text.Json.Nodes.JsonObject obj)
    {
        foreach (var (key, value) in obj)
        {
            if (key.Equals("Enabled", StringComparison.OrdinalIgnoreCase) && value is System.Text.Json.Nodes.JsonValue)
            {
                obj[key] = true;
            }
            else if (value is System.Text.Json.Nodes.JsonObject child)
            {
                FlipEnabledToTrue(child);
            }
        }
    }
}

/// <summary>
/// Sets a config value (surgewave config set key value)
/// </summary>
internal sealed class ConfigSetCommand : Command
{
    private readonly Argument<string> _keyArg = new("key") { Description = "Configuration key to set" };
    private readonly Argument<string> _valueArg = new("value") { Description = "Value to set" };

    public ConfigSetCommand() : base("set", "Set a configuration value")
    {
        Arguments.Add(_keyArg);
        Arguments.Add(_valueArg);
        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var key = parseResult.GetValue(_keyArg)!;
            var value = parseResult.GetValue(_valueArg)!;
            Execute(key, value);
            return Task.FromResult(0);
        });
    }

    private static void Execute(string key, string value)
    {
        var config = SurgewaveConfig.Load();

        switch (key.ToLowerInvariant())
        {
            case "broker":
                config.BootstrapServer = value;
                break;
            case "format":
                config.Format = value;
                break;
            case "verbose":
                config.Verbose = bool.TryParse(value, out var v) ? v : null;
                break;
            case "timeout":
                config.Timeout = int.TryParse(value, out var t) ? t : null;
                break;
            case "profile":
                config.ActiveProfile = value;
                break;
            default:
                AnsiConsole.MarkupLine($"[red]Unknown configuration key: {key}[/]");
                AnsiConsole.MarkupLine("[dim]Valid keys: broker, format, verbose, timeout, profile[/]");
                return;
        }

        config.Save();
        AnsiConsole.MarkupLine($"[green]Set {key} = {value}[/]");
    }
}

/// <summary>
/// Gets a config value (surgewave config get key)
/// </summary>
internal sealed class ConfigGetCommand : Command
{
    private readonly Argument<string> _keyArg = new("key") { Description = "Configuration key to get" };

    public ConfigGetCommand() : base("get", "Get a configuration value")
    {
        Arguments.Add(_keyArg);
        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var key = parseResult.GetValue(_keyArg)!;
            Execute(key);
            return Task.FromResult(0);
        });
    }

    private static void Execute(string key)
    {
        var config = SurgewaveConfig.Load().GetEffective();

        var value = key.ToLowerInvariant() switch
        {
            "broker" => config.BootstrapServer,
            "format" => config.Format,
            "verbose" => config.Verbose?.ToString()?.ToLowerInvariant(),
            "timeout" => config.Timeout?.ToString(),
            "profile" => config.ActiveProfile,
            _ => null
        };

        if (value != null)
        {
            System.Console.WriteLine(value);
        }
        else
        {
            AnsiConsole.MarkupLine($"[yellow]Key '{key}' not set or unknown[/]");
        }
    }
}

/// <summary>
/// Manages profiles (surgewave config profile)
/// </summary>
internal sealed class ConfigProfileCommand : Command
{
    public ConfigProfileCommand() : base("profile", "Manage configuration profiles")
    {
        Subcommands.Add(new ProfileListCommand());
        Subcommands.Add(new ProfileUseCommand());
        Subcommands.Add(new ProfileAddCommand());
        Subcommands.Add(new ProfileRemoveCommand());
    }
}

internal sealed class ProfileListCommand : Command
{
    public ProfileListCommand() : base("list", "List available profiles")
    {
        this.SetAction((ParseResult _, CancellationToken _) =>
        {
            Execute();
            return Task.FromResult(0);
        });
    }

    private static void Execute()
    {
        var config = SurgewaveConfig.Load();

        if (config.Profiles == null || config.Profiles.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No profiles configured[/]");
            return;
        }

        foreach (var (name, profile) in config.Profiles)
        {
            var active = name == config.ActiveProfile ? " (active)" : "";
            System.Console.WriteLine($"{name}{active}: {profile.BootstrapServer ?? "default"}");
        }
    }
}

internal sealed class ProfileUseCommand : Command
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Profile name to activate" };

    public ProfileUseCommand() : base("use", "Switch to a profile")
    {
        Arguments.Add(_nameArg);
        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var name = parseResult.GetValue(_nameArg)!;
            Execute(name);
            return Task.FromResult(0);
        });
    }

    private static void Execute(string name)
    {
        var config = SurgewaveConfig.Load();

        if (config.Profiles == null || !config.Profiles.TryGetValue(name, out var profile))
        {
            AnsiConsole.MarkupLine($"[red]Profile '{name}' not found[/]");
            return;
        }

        config.ActiveProfile = name;
        config.Save();

        AnsiConsole.MarkupLine($"[green]Switched to profile '{name}'[/]");
        if (!string.IsNullOrEmpty(profile.BootstrapServer))
        {
            AnsiConsole.MarkupLine($"[dim]Bootstrap server: {profile.BootstrapServer}[/]");
        }
    }
}

internal sealed class ProfileAddCommand : Command
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Profile name" };
    private readonly Option<string?> _brokerOpt = new("--broker", "-b") { Description = "Bootstrap server for this profile" };

    public ProfileAddCommand() : base("add", "Add a new profile")
    {
        Arguments.Add(_nameArg);
        Options.Add(_brokerOpt);
        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var name = parseResult.GetValue(_nameArg)!;
            var broker = parseResult.GetValue(_brokerOpt);
            Execute(name, broker);
            return Task.FromResult(0);
        });
    }

    private static void Execute(string name, string? broker)
    {
        var config = SurgewaveConfig.Load();
        config.Profiles ??= new Dictionary<string, SurgewaveProfile>();

        if (config.Profiles.ContainsKey(name))
        {
            AnsiConsole.MarkupLine($"[yellow]Profile '{name}' already exists, updating...[/]");
        }

        config.Profiles[name] = new SurgewaveProfile
        {
            BootstrapServer = broker
        };

        config.Save();
        AnsiConsole.MarkupLine($"[green]Added profile '{name}'[/]");
    }
}

internal sealed class ProfileRemoveCommand : Command
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Profile name to remove" };

    public ProfileRemoveCommand() : base("remove", "Remove a profile")
    {
        Arguments.Add(_nameArg);
        this.SetAction((ParseResult parseResult, CancellationToken _) =>
        {
            var name = parseResult.GetValue(_nameArg)!;
            Execute(name);
            return Task.FromResult(0);
        });
    }

    private static void Execute(string name)
    {
        var config = SurgewaveConfig.Load();

        if (config.Profiles == null || !config.Profiles.Remove(name))
        {
            AnsiConsole.MarkupLine($"[yellow]Profile '{name}' not found[/]");
            return;
        }

        if (config.ActiveProfile == name)
        {
            config.ActiveProfile = null;
        }

        config.Save();
        AnsiConsole.MarkupLine($"[green]Removed profile '{name}'[/]");
    }
}
