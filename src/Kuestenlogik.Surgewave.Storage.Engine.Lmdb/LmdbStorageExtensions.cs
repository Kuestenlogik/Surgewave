using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Lmdb;

/// <summary>
/// Extension methods for configuring LMDB storage on SurgewaveRuntimeBuilder.
/// </summary>
public static class LmdbStorageExtensions
{
    /// <summary>
    /// Configure LMDB storage with default settings.
    /// Memory-mapped B+Tree with extremely fast reads, ACID transactions.
    /// Ideal for read-heavy workloads with moderate write rates.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithLmdbStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => LmdbLogSegmentFactory.Create());
    }

    /// <summary>
    /// Configure LMDB storage with a custom buffer pool.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithLmdbStorage(this SurgewaveRuntimeBuilder builder, ISurgewaveBufferPool bufferPool)
    {
        return builder.WithStorage(() => LmdbLogSegmentFactory.Create(bufferPool));
    }
}
