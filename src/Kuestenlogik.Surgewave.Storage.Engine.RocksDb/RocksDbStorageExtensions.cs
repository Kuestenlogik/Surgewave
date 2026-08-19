using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.RocksDb;

/// <summary>
/// Extension methods for configuring RocksDB storage on any storage-configurable runtime builder.
/// </summary>
public static class RocksDbStorageExtensions
{
    /// <summary>
    /// Configure RocksDB storage with default settings.
    /// LSM-Tree based storage optimized for write-heavy workloads.
    /// </summary>
    public static TBuilder WithRocksDbStorage<TBuilder>(
        this TBuilder builder)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => RocksDbLogSegmentFactory.Create());
        return builder;
    }

    /// <summary>
    /// Configure RocksDB storage with a custom buffer pool.
    /// </summary>
    public static TBuilder WithRocksDbStorage<TBuilder>(
        this TBuilder builder, ISurgewaveBufferPool bufferPool)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => RocksDbLogSegmentFactory.Create(bufferPool));
        return builder;
    }
}
