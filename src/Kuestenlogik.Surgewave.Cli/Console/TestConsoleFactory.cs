using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Console;

/// <summary>
/// Factory for creating IAnsiConsole instances suitable for testing.
/// </summary>
public static class TestConsoleFactory
{
    /// <summary>
    /// Creates a recording console that captures all output to a StringWriter.
    /// </summary>
    /// <returns>A tuple containing the console and the output writer.</returns>
    public static (IAnsiConsole Console, StringWriter Output) CreateRecording()
    {
        var output = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(output),
            Interactive = InteractionSupport.No,
            Ansi = AnsiSupport.No
        });
        return (console, output);
    }

    /// <summary>
    /// Creates a recording console with the specified width.
    /// </summary>
    public static (IAnsiConsole Console, StringWriter Output) CreateRecording(int width)
    {
        var output = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(output),
            Interactive = InteractionSupport.No,
            Ansi = AnsiSupport.No
        });

        // Set console width for consistent table rendering
        console.Profile.Width = width;

        return (console, output);
    }
}
