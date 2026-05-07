using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;

namespace Kuestenlogik.Surgewave.Runtime;

/// <summary>
/// Extension methods for configuring storage backends on SurgewaveRuntimeBuilder.
/// Provides a fluent API similar to EF Core's database configuration.
/// </summary>
public static class SurgewaveStorageExtensions
{
    /// <summary>
    /// Configure in-memory storage (ephemeral, no persistence).
    /// Ideal for testing and development.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithMemoryStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => new MemoryLogSegmentFactory());
    }

    /// <summary>
    /// Configure zero-copy memory storage (ephemeral, no persistence).
    /// Optimized for minimal memory copies.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithZeroCopyMemoryStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => ZeroCopyMemoryLogSegmentFactory.Create());
    }

    /// <summary>
    /// Configure file-based storage with memory-mapped files.
    /// Default for production with disk persistence.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithFileStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => FileLogSegmentFactory.Create(useMmap: true));
    }

    /// <summary>
    /// Configure file-based storage with optional memory-mapping.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="useMmap">Enable memory-mapped files for zero-copy reads.</param>
    public static SurgewaveRuntimeBuilder WithFileStorage(this SurgewaveRuntimeBuilder builder, bool useMmap)
    {
        return builder.WithStorage(() => FileLogSegmentFactory.Create(useMmap: useMmap));
    }

    // Arrow storage extension methods have moved to the Surgewave.Storage.Arrow enterprise plugin.
    // Use: builder.WithStorage(() => new ArrowLogSegmentFactory(config))
}
