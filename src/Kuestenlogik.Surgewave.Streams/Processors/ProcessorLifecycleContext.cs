namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Context provided to processor lifecycle hooks.
/// </summary>
public sealed record ProcessorLifecycleContext
{
    public required string NodeName { get; init; }
    public required ProcessorContext ProcessorContext { get; init; }
    public CancellationToken ShutdownToken { get; init; }
    public TimeSpan ShutdownTimeout { get; init; }
}
