using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Factory for creating object-store-backed log segments.
/// Bridges ObjectStoreEngineFactory to the ILogSegmentFactory interface
/// used by the LogManager and broker infrastructure.
/// </summary>
public sealed class ObjectStoreLogSegmentFactory : ILogSegmentFactory
{
    private readonly ObjectStoreEngineFactory _engineFactory;

    /// <summary>
    /// Object store provides durable remote storage.
    /// </summary>
    public bool IsPersistent => true;

    public ObjectStoreLogSegmentFactory(ObjectStoreEngineFactory engineFactory)
    {
        _engineFactory = engineFactory ?? throw new ArgumentNullException(nameof(engineFactory));
    }

    /// <summary>
    /// Create an ObjectStoreLogSegmentFactory with the given provider and config.
    /// </summary>
    public static ObjectStoreLogSegmentFactory Create(
        IObjectStoreProvider storeProvider,
        ObjectStoreConfig? config = null,
        ISurgewaveBufferPool? bufferPool = null)
    {
        config ??= new ObjectStoreConfig();
        var engineFactory = new ObjectStoreEngineFactory(storeProvider, config, bufferPool);
        return new ObjectStoreLogSegmentFactory(engineFactory);
    }

    public ILogSegment CreateSegment(
        string baseDirectory,
        long baseOffset,
        bool createNew,
        long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
#pragma warning disable CA2000 // Engine ownership is transferred to the adapter
        ISurgewaveStorageEngine engine = createNew
            ? _engineFactory.Create(baseDirectory, baseOffset, maxSegmentSize)
            : _engineFactory.Open(baseDirectory, baseOffset);

        return new StorageEngineSegmentAdapter(engine);
#pragma warning restore CA2000
    }
}
