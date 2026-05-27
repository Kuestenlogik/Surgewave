using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Print just the bundled <c>pluginsettings.json</c> of an installed plugin
/// (surgewave plugin defaults &lt;id&gt;).
///
/// <para>
/// This is a focused subset of <c>surgewave plugin show</c> for the common
/// "what defaults does this plugin contribute to my broker config?" question.
/// The output is the literal JSON content of the plugin's settings file —
/// suitable for piping into <c>jq</c>, copying into a user appsettings.json
/// as a starting point, or diffing against the previous version.
/// </para>
/// </summary>
public class PluginDefaultsCommand : CommandBase
{
    private static readonly JsonSerializerOptions s_indented = new() { WriteIndented = true };

    private readonly Argument<string> _idArg = new("id")
    {
        Description = "Plugin ID (as listed by 'surgewave plugin list')",
    };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Plugins directory to scan",
        DefaultValueFactory = _ => "plugins",
    };

    public PluginDefaultsCommand() : base("defaults", "Print the bundled pluginsettings.json of an installed plugin")
    {
        Arguments.Add(_idArg);
        Options.Add(_directoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var id = parseResult.GetValue(_idArg);
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var pluginsDir = Path.GetFullPath(directory);

        if (string.IsNullOrWhiteSpace(id))
        {
            WriteError("Plugin id is required.");
            return 1;
        }

        if (!Directory.Exists(pluginsDir))
        {
            WriteError($"Plugins directory does not exist: {pluginsDir}");
            return 1;
        }

        var manager = new PluginPackageManager();
        InstalledPlugin? match = null;
        await foreach (var plugin in manager.GetInstalledPluginsAsync(pluginsDir, ct))
        {
            if (string.Equals(plugin.Id, id, StringComparison.OrdinalIgnoreCase))
            {
                match = plugin;
                break;
            }
        }

        if (match is null)
        {
            WriteError($"Plugin '{id}' not installed in {pluginsDir}");
            return 1;
        }

        var manifest = match.Manifest!;
        var settingsFileName = string.IsNullOrWhiteSpace(manifest.PluginSettings)
            ? "pluginsettings.json"
            : manifest.PluginSettings;
        var settingsPath = Path.Combine(match.InstallPath!, settingsFileName);

        if (!File.Exists(settingsPath))
        {
            WriteWarning($"Plugin '{id}' does not bundle a {settingsFileName} (uses C# property defaults only).");
            return 1;
        }

        // Print raw content. Stay quiet so the output is pipe-friendly: 'surgewave plugin
        // defaults mqtt | jq .Surgewave.Mqtt' should just work.
        var content = await File.ReadAllTextAsync(settingsPath, ct);

        // Round-trip through JsonDocument to normalise whitespace and validate it parses.
        try
        {
            using var doc = JsonDocument.Parse(content);
            var formatted = JsonSerializer.Serialize(doc.RootElement, s_indented);
            System.Console.WriteLine(formatted);
        }
        catch (JsonException ex)
        {
            WriteError($"Plugin settings file is not valid JSON: {ex.Message}");
            System.Console.WriteLine(content);
            return 1;
        }

        return 0;
    }
}
