using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Extension methods to integrate chaos fault injection into the Surgewave runtime.
/// </summary>
public static class ChaosRuntimeExtensions
{
    /// <summary>
    /// Wraps the storage factory with a <see cref="ChaosLogSegmentFactory"/> that injects faults
    /// controlled by the specified <see cref="ChaosEngine"/>.
    /// Uses an in-memory storage backend by default.
    /// </summary>
    /// <param name="builder">The runtime builder to configure.</param>
    /// <param name="engine">The chaos engine controlling fault injection.</param>
    /// <param name="loggerFactory">Optional logger factory for diagnostics.</param>
    /// <returns>The builder for fluent chaining.</returns>
    public static SurgewaveRuntimeBuilder WithChaosEngine(this SurgewaveRuntimeBuilder builder, ChaosEngine engine, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(engine);

        // Wrap a memory-based storage factory with chaos fault injection.
        // For file-based storage, use WithChaosStorage instead.
        var innerFactory = new MemoryLogSegmentFactory();
        var logger = loggerFactory ?? NullLoggerFactory.Instance;
        var chaosFactory = new ChaosLogSegmentFactory(innerFactory, engine, brokerId: 0, logger);
        builder.WithStorage(chaosFactory);
        return builder;
    }

    /// <summary>
    /// Wraps the storage factory with a <see cref="ChaosLogSegmentFactory"/> that injects faults,
    /// using a specific inner factory for storage.
    /// </summary>
    /// <param name="builder">The runtime builder to configure.</param>
    /// <param name="engine">The chaos engine controlling fault injection.</param>
    /// <param name="innerFactory">The storage factory to wrap with chaos injection.</param>
    /// <param name="brokerId">The broker ID for fault scoping. Defaults to 0.</param>
    /// <param name="loggerFactory">Optional logger factory for diagnostics.</param>
    /// <returns>The builder for fluent chaining.</returns>
    public static SurgewaveRuntimeBuilder WithChaosStorage(this SurgewaveRuntimeBuilder builder, ChaosEngine engine,
        ILogSegmentFactory innerFactory, int brokerId = 0, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(innerFactory);

        var chaosFactory = new ChaosLogSegmentFactory(innerFactory, engine, brokerId, loggerFactory);
        builder.WithStorage(chaosFactory);
        return builder;
    }
}
