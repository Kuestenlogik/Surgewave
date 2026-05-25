using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.RocksDb;

/// <summary>
/// Extension methods for configuring RocksDB storage on SurgewaveRuntimeBuilder.
/// </summary>
public static class RocksDbStorageExtensions
{
    /// <summary>
    /// Configure RocksDB storage with default settings.
    /// LSM-Tree based storage optimized for write-heavy workloads.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithRocksDbStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => RocksDbLogSegmentFactory.Create());
    }

    /// <summary>
    /// Configure RocksDB storage with a custom buffer pool.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithRocksDbStorage(this SurgewaveRuntimeBuilder builder, ISurgewaveBufferPool bufferPool)
    {
        return builder.WithStorage(() => RocksDbLogSegmentFactory.Create(bufferPool));
    }
}
