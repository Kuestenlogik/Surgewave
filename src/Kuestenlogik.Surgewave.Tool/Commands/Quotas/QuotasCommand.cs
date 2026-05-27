using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Quotas;

internal static class QuotasJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for quota management operations (surgewave quotas ...)
/// </summary>
public class QuotasCommand : CommandBase
{
    public QuotasCommand() : base("quotas", "Quota management operations (produce/fetch rate limiting)")
    {
        Aliases.Add("quota");
        Subcommands.Add(new QuotaConfigCommand());
        Subcommands.Add(new QuotaClientsCommand());
    }
}

/// <summary>
/// Quota config operations (surgewave quotas config)
/// </summary>
public class QuotaConfigCommand : CommandBase
{
    public QuotaConfigCommand() : base("config", "View or modify quota configuration")
    {
        Subcommands.Add(new QuotaConfigShowCommand());
        Subcommands.Add(new QuotaConfigSetCommand());

        // Default handler shows config
        this.SetAction(ExecuteShowAsync);
    }

    private static async Task<int> ExecuteShowAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var config = await client.Admin.GetQuotaConfigAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    config.Enabled,
                    config.ProduceRateLimit,
                    config.FetchRateLimit,
                    config.RequestRateLimit
                };
                Console.WriteLine(JsonSerializer.Serialize(output, QuotasJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"Enabled: {config.Enabled}");
                Console.WriteLine($"ProduceRateLimit: {FormatBytes(config.ProduceRateLimit)}/s");
                Console.WriteLine($"FetchRateLimit: {FormatBytes(config.FetchRateLimit)}/s");
                Console.WriteLine($"RequestRateLimit: {config.RequestRateLimit}/s");
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Quota Configuration[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                var statusColor = config.Enabled ? "green" : "red";
                var statusText = config.Enabled ? "Enabled" : "Disabled";

                grid.AddRow("[bold]Status:[/]", $"[{statusColor}]{statusText}[/]");
                grid.AddRow("[bold]Produce Rate Limit:[/]", $"[cyan]{FormatBytes(config.ProduceRateLimit)}[/]/s");
                grid.AddRow("[bold]Fetch Rate Limit:[/]", $"[cyan]{FormatBytes(config.FetchRateLimit)}[/]/s");
                grid.AddRow("[bold]Request Rate Limit:[/]", $"[cyan]{config.RequestRateLimit:N0}[/] req/s");

                AnsiConsole.Write(grid);
                AnsiConsole.WriteLine();
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get quota config: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }
}

/// <summary>
/// Show quota config (surgewave quotas config show)
/// </summary>
public class QuotaConfigShowCommand : CommandBase
{
    public QuotaConfigShowCommand() : base("show", "Show current quota configuration")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var config = await client.Admin.GetQuotaConfigAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    config.Enabled,
                    config.ProduceRateLimit,
                    config.FetchRateLimit,
                    config.RequestRateLimit
                };
                Console.WriteLine(JsonSerializer.Serialize(output, QuotasJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"Enabled: {config.Enabled}");
                Console.WriteLine($"ProduceRateLimit: {FormatBytes(config.ProduceRateLimit)}/s");
                Console.WriteLine($"FetchRateLimit: {FormatBytes(config.FetchRateLimit)}/s");
                Console.WriteLine($"RequestRateLimit: {config.RequestRateLimit}/s");
            }
            else
            {
                AnsiConsole.Write(new Rule("[bold blue]Quota Configuration[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                var statusColor = config.Enabled ? "green" : "red";
                var statusText = config.Enabled ? "Enabled" : "Disabled";

                grid.AddRow("[bold]Status:[/]", $"[{statusColor}]{statusText}[/]");
                grid.AddRow("[bold]Produce Rate Limit:[/]", $"[cyan]{FormatBytes(config.ProduceRateLimit)}[/]/s");
                grid.AddRow("[bold]Fetch Rate Limit:[/]", $"[cyan]{FormatBytes(config.FetchRateLimit)}[/]/s");
                grid.AddRow("[bold]Request Rate Limit:[/]", $"[cyan]{config.RequestRateLimit:N0}[/] req/s");

                AnsiConsole.Write(grid);
                AnsiConsole.WriteLine();
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get quota config: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }
}

/// <summary>
/// Set quota config (surgewave quotas config set)
/// </summary>
public class QuotaConfigSetCommand : CommandBase
{
    private readonly Option<long?> _produceRateOption = new("--produce-rate") { Description = "Produce rate limit in bytes/second" };
    private readonly Option<long?> _fetchRateOption = new("--fetch-rate") { Description = "Fetch rate limit in bytes/second" };
    private readonly Option<long?> _requestRateOption = new("--request-rate") { Description = "Request rate limit in requests/second" };
    private readonly Option<bool?> _enabledOption = new("--enabled") { Description = "Enable or disable quotas" };

    public QuotaConfigSetCommand() : base("set", "Update quota configuration")
    {
        Options.Add(_produceRateOption);
        Options.Add(_fetchRateOption);
        Options.Add(_requestRateOption);
        Options.Add(_enabledOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));

        var produceRate = parseResult.GetValue(_produceRateOption);
        var fetchRate = parseResult.GetValue(_fetchRateOption);
        var requestRate = parseResult.GetValue(_requestRateOption);
        var enabled = parseResult.GetValue(_enabledOption);

        if (!produceRate.HasValue && !fetchRate.HasValue && !requestRate.HasValue && !enabled.HasValue)
        {
            WriteError("At least one option must be specified (--produce-rate, --fetch-rate, --request-rate, or --enabled)");
            return 1;
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Admin.SetQuotaConfigAsync(
                produceRate,
                fetchRate,
                requestRate,
                enabled,
                ct);

            WriteSuccess("Quota configuration updated successfully.");

            // Show new config
            var config = await client.Admin.GetQuotaConfigAsync(ct);
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[dim]Status:[/] {(config.Enabled ? "[green]Enabled[/]" : "[red]Disabled[/]")}");
            AnsiConsole.MarkupLine($"[dim]Produce:[/] {FormatBytes(config.ProduceRateLimit)}/s");
            AnsiConsole.MarkupLine($"[dim]Fetch:[/] {FormatBytes(config.FetchRateLimit)}/s");
            AnsiConsole.MarkupLine($"[dim]Requests:[/] {config.RequestRateLimit:N0}/s");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to set quota config: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }
}

/// <summary>
/// Quota clients operations (surgewave quotas clients)
/// </summary>
public class QuotaClientsCommand : CommandBase
{
    public QuotaClientsCommand() : base("clients", "View client quota usage")
    {
        Subcommands.Add(new QuotaClientsListCommand());
        Subcommands.Add(new QuotaClientsDescribeCommand());

        // Default handler lists clients
        this.SetAction(ExecuteListAsync);
    }

    private static async Task<int> ExecuteListAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var clients = await client.Admin.ListClientQuotasAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = clients.Select(c => new
                {
                    c.ClientId,
                    c.ProduceRate,
                    c.FetchRate,
                    c.IsThrottled
                });
                Console.WriteLine(JsonSerializer.Serialize(output, QuotasJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var c in clients)
                {
                    Console.WriteLine($"{c.ClientId}\t{c.ProduceRate}\t{c.FetchRate}\t{c.IsThrottled}");
                }
            }
            else
            {
                if (clients.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No clients with quota tracking.[/]");
                    return 0;
                }

                AnsiConsole.Write(new Rule("[bold blue]Client Quota Usage[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Client ID");
                table.AddColumn("Produce Rate", c => c.Alignment(Justify.Right));
                table.AddColumn("Fetch Rate", c => c.Alignment(Justify.Right));
                table.AddColumn("Status");

                foreach (var c in clients)
                {
                    var statusColor = c.IsThrottled ? "red" : "green";
                    var statusText = c.IsThrottled ? "Throttled" : "OK";

                    table.AddRow(
                        $"[cyan]{c.ClientId}[/]",
                        FormatBytes(c.ProduceRate) + "/s",
                        FormatBytes(c.FetchRate) + "/s",
                        $"[{statusColor}]{statusText}[/]");
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Total clients: {clients.Count}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list client quotas: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }
}

/// <summary>
/// List clients with quota tracking (surgewave quotas clients list)
/// </summary>
public class QuotaClientsListCommand : CommandBase
{
    public QuotaClientsListCommand() : base("list", "List clients with quota tracking")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var clients = await client.Admin.ListClientQuotasAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = clients.Select(c => new
                {
                    c.ClientId,
                    c.ProduceRate,
                    c.FetchRate,
                    c.IsThrottled
                });
                Console.WriteLine(JsonSerializer.Serialize(output, QuotasJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var c in clients)
                {
                    Console.WriteLine($"{c.ClientId}\t{c.ProduceRate}\t{c.FetchRate}\t{c.IsThrottled}");
                }
            }
            else
            {
                if (clients.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No clients with quota tracking.[/]");
                    return 0;
                }

                AnsiConsole.Write(new Rule("[bold blue]Client Quota Usage[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Client ID");
                table.AddColumn("Produce Rate", c => c.Alignment(Justify.Right));
                table.AddColumn("Fetch Rate", c => c.Alignment(Justify.Right));
                table.AddColumn("Status");

                foreach (var c in clients)
                {
                    var statusColor = c.IsThrottled ? "red" : "green";
                    var statusText = c.IsThrottled ? "Throttled" : "OK";

                    table.AddRow(
                        $"[cyan]{c.ClientId}[/]",
                        FormatBytes(c.ProduceRate) + "/s",
                        FormatBytes(c.FetchRate) + "/s",
                        $"[{statusColor}]{statusText}[/]");
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Total clients: {clients.Count}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list client quotas: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }
}

/// <summary>
/// Describe client quota (surgewave quotas clients describe)
/// </summary>
public class QuotaClientsDescribeCommand : CommandBase
{
    private readonly Argument<string[]> _clientIdsArg = new("client-ids") { Description = "The client IDs to describe", Arity = ArgumentArity.OneOrMore };

    public QuotaClientsDescribeCommand() : base("describe", "Describe client quota usage")
    {
        Aliases.Add("show");
        Arguments.Add(_clientIdsArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var clientIds = parseResult.GetValue(_clientIdsArg)?.ToList() ?? [];

        if (clientIds.Count == 0)
        {
            WriteError("At least one client ID must be specified");
            return 1;
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var descriptions = await client.Admin.DescribeClientQuotasAsync(clientIds, ct);

            if (format == OutputFormat.Json)
            {
                var output = descriptions.Select(d => new
                {
                    d.ClientId,
                    d.ProduceRate,
                    d.FetchRate,
                    d.ProduceTokensAvailable,
                    d.FetchTokensAvailable,
                    d.IsThrottled,
                    d.LastActivityMs
                });
                Console.WriteLine(JsonSerializer.Serialize(output, QuotasJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var d in descriptions)
                {
                    Console.WriteLine($"ClientId: {d.ClientId}");
                    Console.WriteLine($"ProduceRate: {d.ProduceRate}");
                    Console.WriteLine($"FetchRate: {d.FetchRate}");
                    Console.WriteLine($"ProduceTokensAvailable: {d.ProduceTokensAvailable}");
                    Console.WriteLine($"FetchTokensAvailable: {d.FetchTokensAvailable}");
                    Console.WriteLine($"IsThrottled: {d.IsThrottled}");
                    Console.WriteLine($"LastActivityMs: {d.LastActivityMs}");
                    Console.WriteLine();
                }
            }
            else
            {
                if (descriptions.Count == 0)
                {
                    WriteError("No matching clients found");
                    return 1;
                }

                foreach (var d in descriptions)
                {
                    AnsiConsole.Write(new Rule($"[bold blue]Client: {d.ClientId}[/]").LeftJustified());
                    AnsiConsole.WriteLine();

                    var grid = new Grid();
                    grid.AddColumn();
                    grid.AddColumn();

                    var statusColor = d.IsThrottled ? "red" : "green";
                    var statusText = d.IsThrottled ? "Throttled" : "OK";

                    grid.AddRow("[bold]Status:[/]", $"[{statusColor}]{statusText}[/]");
                    grid.AddRow("[bold]Produce Rate:[/]", FormatBytes(d.ProduceRate) + "/s");
                    grid.AddRow("[bold]Fetch Rate:[/]", FormatBytes(d.FetchRate) + "/s");
                    grid.AddRow("[bold]Produce Tokens Available:[/]", FormatBytes(d.ProduceTokensAvailable));
                    grid.AddRow("[bold]Fetch Tokens Available:[/]", FormatBytes(d.FetchTokensAvailable));

                    if (d.LastActivityMs > 0)
                    {
                        var lastActivity = DateTimeOffset.FromUnixTimeMilliseconds(d.LastActivityMs);
                        var ago = DateTimeOffset.UtcNow - lastActivity;
                        grid.AddRow("[bold]Last Activity:[/]", $"{FormatTimeAgo(ago)} ago");
                    }

                    AnsiConsole.Write(grid);
                    AnsiConsole.WriteLine();
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe client quotas: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1_000_000_000) return $"{bytes / 1_000_000_000.0:F1} GB";
        if (bytes >= 1_000_000) return $"{bytes / 1_000_000.0:F1} MB";
        if (bytes >= 1_000) return $"{bytes / 1_000.0:F1} KB";
        return $"{bytes} B";
    }

    private static string FormatTimeAgo(TimeSpan ts)
    {
        if (ts.TotalDays >= 1) return $"{ts.TotalDays:F0}d";
        if (ts.TotalHours >= 1) return $"{ts.TotalHours:F0}h";
        if (ts.TotalMinutes >= 1) return $"{ts.TotalMinutes:F0}m";
        return $"{ts.TotalSeconds:F0}s";
    }
}
