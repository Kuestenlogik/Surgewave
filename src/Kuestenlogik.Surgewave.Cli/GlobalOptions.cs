using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli;

/// <summary>
/// Global options available to all commands
/// </summary>
public static class GlobalOptions
{
    /// <summary>
    /// Bootstrap servers option (similar to kafka-topics.sh --bootstrap-server)
    /// Also supports --broker as a simpler alias for Surgewave users
    /// </summary>
    public static readonly Option<string> BootstrapServers = new("--bootstrap-server", "--broker", "-b")
    {
        Description = "The Surgewave broker to connect to",
        DefaultValueFactory = _ => "localhost:9092"
    };

    /// <summary>
    /// Verbose output option
    /// </summary>
    public static readonly Option<bool> Verbose = new("--verbose", "-v")
    {
        Description = "Show detailed output",
        DefaultValueFactory = _ => false
    };

    /// <summary>
    /// Output format option
    /// </summary>
    public static readonly Option<OutputFormat> Format = new("--format", "-f")
    {
        Description = "Output format (table, json, plain)",
        DefaultValueFactory = _ => OutputFormat.Table
    };

    /// <summary>
    /// Timeout option in milliseconds
    /// </summary>
    public static readonly Option<int> Timeout = new("--timeout", "-t")
    {
        Description = "Request timeout in milliseconds",
        DefaultValueFactory = _ => 30000
    };

    /// <summary>
    /// Skip confirmation prompts for destructive operations (delete, remove,
    /// decommission, restore). Equivalent to <c>--force</c> on individual
    /// destructive commands but available globally — set once for the whole
    /// CLI invocation. Mirrors the <c>--yes</c> flag from kubectl / gh / aws.
    /// </summary>
    public static readonly Option<bool> AssumeYes = new("--yes", "-y")
    {
        Description = "Skip confirmation prompts for destructive operations",
        DefaultValueFactory = _ => false,
    };
}

public enum OutputFormat
{
    Table,
    Json,
    Plain
}
