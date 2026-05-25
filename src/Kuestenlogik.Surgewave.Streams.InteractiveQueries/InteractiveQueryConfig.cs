namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Configuration for the Interactive Query Service.
/// Bind from configuration section <c>Surgewave:Streams:InteractiveQueries</c>.
/// </summary>
public sealed class InteractiveQueryConfig
{
    /// <summary>
    /// Gets whether the Interactive Query Service is enabled.
    /// Default: <c>false</c>.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Gets the maximum number of store entries returned in a single page.
    /// Default: <c>1000</c>.
    /// </summary>
    public int MaxEntriesPerPage { get; init; } = 1000;
}
