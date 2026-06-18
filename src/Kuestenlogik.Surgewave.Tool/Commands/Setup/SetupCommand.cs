using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Marketplace;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Setup;

/// <summary>
/// `surgewave setup` — interactive wizard that walks the operator
/// through the four big choices (storage engine, protocols, schema
/// handlers, connectors, auth, telemetry) and writes
/// <c>setup.sh</c> + <c>setup.ps1</c> + <c>appsettings.json</c> to
/// the chosen output directory. Operators commit the three files
/// into their deployment repo; the scripts can later be replayed on
/// any broker host with the same answers.
///
/// The prompts use Spectre.Console; the file-generation logic lives
/// in <see cref="SetupScriptGenerator"/> + <see cref="AppSettingsGenerator"/>
/// so it is unit-testable without driving stdin.
/// </summary>
public sealed class SetupCommand : CommandBase
{
    private static readonly string[] SkipChoice = ["(skip)"];

    private readonly Option<string> _outputDirOpt = new("--output-dir", "-o")
    {
        Description = "Directory to write setup.sh / setup.ps1 / appsettings.json into. Default: current dir.",
        DefaultValueFactory = _ => Environment.CurrentDirectory,
    };

    private readonly Option<bool> _nonInteractiveOpt = new("--non-interactive")
    {
        Description = "Skip prompts and emit a default 'none-selected' setup. Useful for CI dry-runs.",
    };

    public SetupCommand() : base("setup", "Interactive deployment wizard — emits setup scripts + appsettings skeleton.")
    {
        Options.Add(_outputDirOpt);
        Options.Add(_nonInteractiveOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var outputDir = parseResult.GetValue(_outputDirOpt) ?? Environment.CurrentDirectory;
        var nonInteractive = parseResult.GetValue(_nonInteractiveOpt);
        Directory.CreateDirectory(outputDir);

        var answers = nonInteractive ? new SetupAnswers() : await RunPromptsAsync(ct);
        WriteOutputs(outputDir, answers);

        AnsiConsole.MarkupLine($"[green]✓[/] Wrote setup.sh, setup.ps1 and appsettings.json to [bold]{outputDir}[/]");
        AnsiConsole.MarkupLine("[dim]Commit the three files into your deployment repo and run setup.sh (or setup.ps1) on the broker host.[/]");
        return 0;
    }

    private static async Task<SetupAnswers> RunPromptsAsync(CancellationToken ct)
    {
        AnsiConsole.MarkupLine("[bold]Surgewave deployment wizard[/]");
        AnsiConsole.MarkupLine("[dim]Loading marketplace…[/]");

        var repos = BuildRepositories();
        var browser = new MarketplaceBrowser(repos);
        var all = await browser.BrowseAsync(cancellationToken: ct);

        var storage      = PickOneOptional(all, PluginCategory.StorageEngine,  "Storage engine plugin (leave blank for built-in default)?");
        var protocols    = PickManyOptional(all, PluginCategory.Protocol,      "Protocol adapters (space to toggle, enter to confirm)");
        var schemas      = PickManyOptional(all, PluginCategory.SchemaHandler, "Schema handlers");
        var connectors   = PickManyOptional(all, PluginCategory.Connector,     "Connectors");

        var auth = AnsiConsole.Prompt(
            new SelectionPrompt<SetupAuthMethod>()
                .Title("Auth method?")
                .AddChoices(Enum.GetValues<SetupAuthMethod>())
                .UseConverter(a => a switch
                {
                    SetupAuthMethod.None       => "None (development only)",
                    SetupAuthMethod.SaslPlain  => "SASL/PLAIN",
                    SetupAuthMethod.SaslScram  => "SASL/SCRAM-SHA-256",
                    SetupAuthMethod.Tls        => "TLS (server cert only)",
                    SetupAuthMethod.MutualTls  => "mTLS (mutual cert authentication)",
                    _                          => a.ToString(),
                }));

        var telemetryEnabled = AnsiConsole.Confirm("Enable OTLP telemetry (traces + metrics)?", defaultValue: true);
        string? otlpEndpoint = null;
        if (telemetryEnabled)
        {
            otlpEndpoint = AnsiConsole.Ask("OTLP endpoint URL?", "http://localhost:4317");
        }

        return new SetupAnswers
        {
            StorageEngine = storage,
            Protocols = protocols,
            SchemaHandlers = schemas,
            Connectors = connectors,
            Auth = auth,
            TelemetryEnabled = telemetryEnabled,
            OtlpEndpoint = otlpEndpoint,
        };
    }

    private static PluginMarketplaceEntry? PickOneOptional(
        IReadOnlyList<PluginMarketplaceEntry> all, PluginCategory category, string title)
    {
        var choices = all.Where(e => e.Category == category).ToList();
        if (choices.Count == 0) return null;

        var prompt = new SelectionPrompt<string>()
            .Title(title)
            .AddChoices(SkipChoice.Concat(choices.Select(c => c.PackageId)));
        var picked = AnsiConsole.Prompt(prompt);
        return picked == "(skip)" ? null : choices.First(c => c.PackageId == picked);
    }

    private static IReadOnlyList<PluginMarketplaceEntry> PickManyOptional(
        IReadOnlyList<PluginMarketplaceEntry> all, PluginCategory category, string title)
    {
        var choices = all.Where(e => e.Category == category).ToList();
        if (choices.Count == 0) return [];

        var picked = AnsiConsole.Prompt(
            new MultiSelectionPrompt<string>()
                .Title(title)
                .NotRequired()
                .AddChoices(choices.Select(c => c.PackageId)));
        return choices.Where(c => picked.Contains(c.PackageId)).ToList();
    }

    private static IReadOnlyList<IConnectorRepository> BuildRepositories()
    {
        // Mirrors the existing CLI default: use whatever repositories the
        // operator has configured via `surgewave plugins source add`. Falls
        // back to nuget.org's azuresearch endpoint when no source has been
        // configured. NuGetConnectorRepository covers the latter case via
        // the standard query URL.
        var config = RepositoryConfiguration.Load();
        var repos = config.CreateRepositories().ToList();
        if (repos.Count == 0)
        {
            repos.Add(new NuGetConnectorRepository("nuget.org", "https://api.nuget.org/v3/index.json"));
        }
        return repos;
    }

    private static void WriteOutputs(string outputDir, SetupAnswers answers)
    {
        File.WriteAllText(Path.Combine(outputDir, "setup.sh"), SetupScriptGenerator.RenderBash(answers));
        File.WriteAllText(Path.Combine(outputDir, "setup.ps1"), SetupScriptGenerator.RenderPowerShell(answers));
        File.WriteAllText(Path.Combine(outputDir, "appsettings.json"), AppSettingsGenerator.Render(answers));
    }
}
