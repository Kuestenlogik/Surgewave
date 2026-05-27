using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Console;

/// <summary>
/// Static accessor for the current IAnsiConsole implementation.
/// Defaults to AnsiConsole.Console for production use.
/// Tests can set a different implementation via SetCurrent().
/// </summary>
public static class ConsoleAccessor
{
    private static IAnsiConsole _current = AnsiConsole.Console;

    /// <summary>
    /// The current console implementation.
    /// </summary>
    public static IAnsiConsole Current => _current;

    /// <summary>
    /// Set the current console implementation.
    /// Primarily used for testing.
    /// </summary>
    public static void SetCurrent(IAnsiConsole console)
    {
        _current = console ?? throw new ArgumentNullException(nameof(console));
    }

    /// <summary>
    /// Reset to the default AnsiConsole.Console implementation.
    /// </summary>
    public static void Reset()
    {
        _current = AnsiConsole.Console;
    }

    /// <summary>
    /// Create a scope that temporarily uses a different console.
    /// Automatically restores the previous console when disposed.
    /// </summary>
    public static IDisposable UseTemporary(IAnsiConsole console)
    {
        return new ConsoleScope(console);
    }

    private sealed class ConsoleScope : IDisposable
    {
        private readonly IAnsiConsole _previous;

        public ConsoleScope(IAnsiConsole console)
        {
            _previous = _current;
            _current = console;
        }

        public void Dispose()
        {
            _current = _previous;
        }
    }
}
