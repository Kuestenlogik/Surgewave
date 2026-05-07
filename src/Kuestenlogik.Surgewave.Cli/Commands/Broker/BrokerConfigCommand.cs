using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Broker;

internal static class BrokerConfigJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Describe broker configuration (surgewave broker config)
/// </summary>
public class BrokerConfigCommand : CommandBase
{
    public BrokerConfigCommand() : base("config", "Manage broker configuration")
    {
        Subcommands.Add(new DescribeBrokerConfigCommand());
        Subcommands.Add(new AlterBrokerConfigCommand());
    }
}

/// <summary>
/// Describe broker configuration (surgewave broker config describe)
/// </summary>
public class DescribeBrokerConfigCommand : CommandBase
{
    private readonly Argument<int?> _brokerIdArg = new("broker-id") { Description = "Broker ID (omit for connected broker)", DefaultValueFactory = _ => null };
    private readonly Option<bool> _allOpt = new("--all", "-a") { Description = "Show all configs including defaults" };

    public DescribeBrokerConfigCommand() : base("describe", "Describe broker configuration")
    {
        Arguments.Add(_brokerIdArg);
        Options.Add(_allOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var brokerId = parseResult.GetValue(_brokerIdArg);
        var showAll = parseResult.GetValue(_allOpt);

        WriteVerbose(parseResult, $"Describing broker configuration...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Use -1 to query the connected broker
            var configs = await client.Admin.DescribeBrokerConfigAsync(
                brokerId ?? -1,
                cancellationToken: ct);

            // Filter to non-default if --all not specified
            var filteredConfigs = showAll
                ? configs
                : configs.Where(c => !c.Value.IsDefault).ToDictionary(c => c.Key, c => c.Value);

            if (format == OutputFormat.Json)
            {
                var output = filteredConfigs.Select(c => new
                {
                    Name = c.Key,
                    c.Value.Value,
                    c.Value.IsDefault,
                    c.Value.IsReadOnly,
                    c.Value.IsSensitive
                }).OrderBy(c => c.Name).ToList();
                Console.WriteLine(JsonSerializer.Serialize(output, BrokerConfigJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var config in filteredConfigs.OrderBy(c => c.Key))
                {
                    Console.WriteLine($"{config.Key}={config.Value.Value}");
                }
            }
            else
            {
                var title = brokerId.HasValue
                    ? $"Broker {brokerId} configuration"
                    : "Connected broker configuration";
                AnsiConsole.MarkupLine($"[bold]{title}[/]");
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Config");
                table.AddColumn("Value");
                table.AddColumn("Type");
                table.AddColumn("Sensitive");

                foreach (var config in filteredConfigs.OrderBy(c => c.Key))
                {
                    var value = config.Value.IsSensitive ? "[dim]********[/]" : (config.Value.Value ?? "[dim]null[/]");
                    var configType = config.Value.IsReadOnly
                        ? "[dim]read-only[/]"
                        : (config.Value.IsDefault ? "[dim]default[/]" : "[green]custom[/]");
                    var sensitive = config.Value.IsSensitive ? "[yellow]yes[/]" : "no";

                    table.AddRow(
                        config.Key,
                        value,
                        configType,
                        sensitive
                    );
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {filteredConfigs.Count} config(s){(showAll ? "" : " (use --all to show defaults)")}[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe broker config: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>
/// Alter broker configuration (surgewave broker config alter)
/// </summary>
public class AlterBrokerConfigCommand : CommandBase
{
    private readonly Option<int?> _brokerIdOpt = new("--broker-id", "-i") { Description = "Broker ID (omit for connected broker)" };
    private readonly Option<string[]> _setOpt = new("--set", "-s") { Description = "Config to set (key=value)", AllowMultipleArgumentsPerToken = true };
    private readonly Option<string[]> _deleteOpt = new("--delete", "-d") { Description = "Config to delete (revert to default)", AllowMultipleArgumentsPerToken = true };

    public AlterBrokerConfigCommand() : base("alter", "Alter broker configuration")
    {
        Options.Add(_brokerIdOpt);
        Options.Add(_setOpt);
        Options.Add(_deleteOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var brokerId = parseResult.GetValue(_brokerIdOpt);
        var setConfigs = parseResult.GetValue(_setOpt) ?? [];
        var deleteConfigs = parseResult.GetValue(_deleteOpt) ?? [];

        if (setConfigs.Length == 0 && deleteConfigs.Length == 0)
        {
            WriteError("Must specify at least one --set or --delete option");
            return 1;
        }

        WriteVerbose(parseResult, $"Altering broker configuration...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var configUpdates = new Dictionary<string, string?>();

            // Parse set configs
            foreach (var config in setConfigs)
            {
                var parts = config.Split('=', 2);
                if (parts.Length != 2)
                {
                    WriteError($"Invalid config format: {config} (expected key=value)");
                    return 1;
                }
                configUpdates[parts[0]] = parts[1];
            }

            // Add delete configs with null value
            foreach (var key in deleteConfigs)
            {
                configUpdates[key] = null;
            }

            var result = await client.Admin.AlterBrokerConfigAsync(
                brokerId ?? -1,
                configUpdates,
                ct);

            if (result != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to alter broker config: {result}");
                WriteWarning("Note: Runtime broker config modification is not currently supported. Config changes must be made in the broker's configuration file.");
                return 1;
            }

            WriteSuccess($"Updated {setConfigs.Length + deleteConfigs.Length} broker config(s)");

            foreach (var config in setConfigs)
            {
                AnsiConsole.MarkupLine($"  [green]+[/] {config}");
            }
            foreach (var config in deleteConfigs)
            {
                AnsiConsole.MarkupLine($"  [red]-[/] {config} (reverted to default)");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to alter broker config: {ex.Message}");
            return 1;
        }
    }
}
