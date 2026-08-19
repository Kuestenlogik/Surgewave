using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// The seam that lets storage engines offer fluent <c>With*Storage()</c> extensions without
/// referencing the runtime. An engine package sits BELOW the broker — a
/// <c>ProjectReference</c> upward to <c>Kuestenlogik.Surgewave.Runtime</c> would put a 4-file
/// engine above the entire broker closure (~40 projects), which is exactly the inversion the
/// LMDB/RocksDB/S3/SQLite engines shipped with until this interface existed. Their extensions
/// are now generic over this interface (<c>WithLmdbStorage&lt;TBuilder&gt;(this TBuilder)</c>),
/// so call sites read the same while the dependency arrow points downward.
/// </summary>
public interface IStorageConfigurableBuilder
{
    /// <summary>
    /// Sets the log segment factory the runtime will use for storage. The typed factory call
    /// inside the delegate is also what forces the engine assembly to load, which is what
    /// triggers its <c>ModuleInitializer</c> registration in <c>StorageRegistry</c> — a
    /// name-based <c>WithStorageEngine("lmdb")</c> alone cannot do that.
    /// </summary>
    void UseStorage(Func<ILogSegmentFactory> factory);
}
