namespace Kuestenlogik.Surgewave.Client.Diagnostics;

/// <summary>
/// Interface for exceptions that can provide recovery suggestions to users.
/// </summary>
public interface IRecoverableException
{
    /// <summary>
    /// Gets a suggestion for how to recover from this error, or null if no suggestion is available.
    /// </summary>
    string? RecoverySuggestion { get; }
}
