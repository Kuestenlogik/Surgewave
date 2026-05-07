namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Represents the current phase of a graceful shutdown sequence.
/// </summary>
public enum ShutdownPhase
{
    /// <summary>Shutdown has been initiated.</summary>
    Starting,

    /// <summary>Active stream tasks are being suspended.</summary>
    TasksSuspending,

    /// <summary>State stores are being flushed to durable storage.</summary>
    StoresFlushing,

    /// <summary>Processor nodes are being stopped in reverse-topological order.</summary>
    NodesStopping,

    /// <summary>Shutdown has completed successfully.</summary>
    Completed
}
