using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Cli.Console;
using Kuestenlogik.Surgewave.Client.Diagnostics;
using Kuestenlogik.Surgewave.Core.Util;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Kuestenlogik.Surgewave.Cli.Commands;

/// <summary>
/// Base class for all CLI commands providing common functionality
/// </summary>
public abstract class CommandBase : Command
{
    protected CommandBase(string name, string? description = null)
        : base(name, description)
    {
    }

    /// <summary>
    /// Gets the current console implementation.
    /// </summary>
    protected static IAnsiConsole Console => ConsoleAccessor.Current;

    /// <summary>
    /// Gets the bootstrap servers from the parse result
    /// </summary>
    protected static string GetBootstrapServers(ParseResult parseResult)
        => parseResult.GetValue(GlobalOptions.BootstrapServers) ?? "localhost:9092";

    /// <summary>
    /// Parses bootstrap server string into host and port
    /// </summary>
    protected static (string host, int port) ParseBootstrapServer(string bootstrapServers)
    {
        var broker = BrokerAddress.ParseFirst(bootstrapServers);
        return (broker.Host, broker.Port);
    }

    /// <summary>
    /// Gets whether verbose output is enabled
    /// </summary>
    protected static bool IsVerbose(ParseResult parseResult)
        => parseResult.GetValue(GlobalOptions.Verbose);

    /// <summary>
    /// Gets the output format
    /// </summary>
    protected static OutputFormat GetFormat(ParseResult parseResult)
        => parseResult.GetValue(GlobalOptions.Format);

    /// <summary>
    /// Gets the timeout in milliseconds
    /// </summary>
    protected static int GetTimeout(ParseResult parseResult)
        => parseResult.GetValue(GlobalOptions.Timeout);

    /// <summary>
    /// Writes an error message to the console
    /// </summary>
    protected static void WriteError(string message)
    {
        Console.WriteError(message);
    }

    /// <summary>
    /// Writes an error message with a recovery suggestion to the console
    /// </summary>
    protected static void WriteError(Exception exception)
    {
        Console.WriteError(exception.Message);

        // Check if the exception provides a recovery suggestion
        if (exception is IRecoverableException recoverable && recoverable.RecoverySuggestion is { } suggestion)
        {
            Console.WriteWarning($"Suggestion: {suggestion}");
        }
    }

    /// <summary>
    /// Writes a success message to the console
    /// </summary>
    protected static void WriteSuccess(string message)
    {
        Console.WriteSuccess(message);
    }

    /// <summary>
    /// Writes a warning message to the console
    /// </summary>
    protected static void WriteWarning(string message)
    {
        Console.WriteWarning(message);
    }

    /// <summary>
    /// Writes an info message to the console (only if verbose)
    /// </summary>
    protected static void WriteVerbose(ParseResult parseResult, string message)
    {
        Console.WriteVerbose(message, IsVerbose(parseResult));
    }

    /// <summary>
    /// Writes a line of text to the console.
    /// </summary>
    protected static void WriteLine(string? text = null)
    {
        if (string.IsNullOrEmpty(text))
            Console.WriteLine();
        else
            Console.WriteLine(text);
    }

    /// <summary>
    /// Writes markup text to the console.
    /// </summary>
    protected static void WriteMarkup(string markup)
    {
        Console.MarkupLine(markup);
    }

    /// <summary>
    /// Writes a renderable object (table, grid, rule, etc.) to the console.
    /// </summary>
    protected static void WriteRenderable(IRenderable renderable)
    {
        Console.Write(renderable);
    }
}
