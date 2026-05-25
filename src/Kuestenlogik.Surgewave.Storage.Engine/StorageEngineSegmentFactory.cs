using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Win32.SafeHandles;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Factory that creates StorageEngineSegmentAdapter instances.
/// Wraps ISurgewaveStorageEngineFactory to provide ILogSegmentFactory interface.
/// </summary>
public sealed class StorageEngineSegmentFactory : ILogSegmentFactory
{
    private readonly ISurgewaveStorageEngineFactory _engineFactory;
    private readonly bool _isPersistent;

    public bool IsPersistent => _isPersistent;

    public StorageEngineSegmentFactory(ISurgewaveStorageEngineFactory engineFactory, bool isPersistent)
    {
        _engineFactory = engineFactory;
        _isPersistent = isPersistent;
    }

    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize)
    {
        var engine = createNew
            ? _engineFactory.Create(baseDirectory, baseOffset, maxSegmentSize)
            : _engineFactory.Open(baseDirectory, baseOffset);

        // Try to get file handle for file-based engines
        SafeFileHandle? fileHandle = null;
        string? logFilePath = null;

        if (_isPersistent)
        {
            logFilePath = Path.Combine(baseDirectory, $"{baseOffset:D20}.log");
        }

        return new StorageEngineSegmentAdapter(engine, logFilePath, fileHandle);
    }
}
