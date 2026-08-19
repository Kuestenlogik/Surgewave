using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Lmdb;

/// <summary>
/// Extension methods for configuring LMDB storage on any storage-configurable runtime builder.
/// </summary>
public static class LmdbStorageExtensions
{
    /// <summary>
    /// Configure LMDB storage with default settings.
    /// Memory-mapped B+Tree with extremely fast reads, ACID transactions.
    /// Ideal for read-heavy workloads with moderate write rates.
    /// </summary>
    public static TBuilder WithLmdbStorage<TBuilder>(
        this TBuilder builder)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => LmdbLogSegmentFactory.Create());
        return builder;
    }

    /// <summary>
    /// Configure LMDB storage with a custom buffer pool.
    /// </summary>
    public static TBuilder WithLmdbStorage<TBuilder>(
        this TBuilder builder, ISurgewaveBufferPool bufferPool)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => LmdbLogSegmentFactory.Create(bufferPool));
        return builder;
    }
}
