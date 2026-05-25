using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Console;

/// <summary>
/// Extension methods for IAnsiConsole that add features not available in the base interface.
/// </summary>
public static class AnsiConsoleExtensions
{
    /// <summary>
    /// Gets whether console output is being redirected (piped).
    /// </summary>
    public static bool IsOutputRedirected(this IAnsiConsole _)
        => System.Console.IsOutputRedirected;

    /// <summary>
    /// Gets whether console input is being redirected (piped).
    /// </summary>
    public static bool IsInputRedirected(this IAnsiConsole _)
        => System.Console.IsInputRedirected;

    /// <summary>
    /// Writes a line to standard error (for progress messages that shouldn't interfere with piped output).
    /// </summary>
    public static void WriteLineToError(this IAnsiConsole _, string message)
        => System.Console.Error.WriteLine(message);

    /// <summary>
    /// Reads a line of input from the console.
    /// </summary>
    public static string? ReadLine(this IAnsiConsole _)
        => System.Console.ReadLine();

    /// <summary>
    /// Writes an error message with appropriate formatting based on redirection.
    /// </summary>
    public static void WriteError(this IAnsiConsole console, string message)
    {
        if (System.Console.IsOutputRedirected)
            System.Console.Error.WriteLine($"Error: {message}");
        else
            console.MarkupLine($"[red]Error:[/] {Markup.Escape(message)}");
    }

    /// <summary>
    /// Writes a success message with appropriate formatting based on redirection.
    /// </summary>
    public static void WriteSuccess(this IAnsiConsole console, string message)
    {
        if (System.Console.IsOutputRedirected)
            System.Console.WriteLine(message);
        else
            console.MarkupLine($"[green]{Markup.Escape(message)}[/]");
    }

    /// <summary>
    /// Writes a warning message with appropriate formatting based on redirection.
    /// </summary>
    public static void WriteWarning(this IAnsiConsole console, string message)
    {
        if (System.Console.IsOutputRedirected)
            System.Console.WriteLine($"Warning: {message}");
        else
            console.MarkupLine($"[yellow]Warning:[/] {Markup.Escape(message)}");
    }

    /// <summary>
    /// Writes a verbose/debug message (only shown when verbose mode is enabled).
    /// </summary>
    public static void WriteVerbose(this IAnsiConsole console, string message, bool isVerbose)
    {
        if (!isVerbose) return;

        if (System.Console.IsOutputRedirected)
            System.Console.WriteLine(message);
        else
            console.MarkupLine($"[dim]{Markup.Escape(message)}[/]");
    }
}
