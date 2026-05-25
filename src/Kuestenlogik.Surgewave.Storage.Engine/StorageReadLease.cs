namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Default implementation of IStorageReadLease.
/// </summary>
public sealed class StorageReadLease : IStorageReadLease
{
    private ISurgewaveBuffer? _data;
    private readonly IReadOnlyList<int> _batchOffsets;

    public ISurgewaveBuffer Data
    {
        get
        {
            ObjectDisposedException.ThrowIf(_data == null, this);
            return _data;
        }
    }

    public IReadOnlyList<int> BatchOffsets => _batchOffsets;

    public StorageReadLease(ISurgewaveBuffer data, IReadOnlyList<int> batchOffsets)
    {
        _data = data;
        _batchOffsets = batchOffsets;
    }

    public ISurgewaveBuffer GetBatch(int index)
    {
        ObjectDisposedException.ThrowIf(_data == null, this);

        if (index < 0 || index >= _batchOffsets.Count)
            throw new ArgumentOutOfRangeException(nameof(index));

        var start = _batchOffsets[index];
        var end = index + 1 < _batchOffsets.Count
            ? _batchOffsets[index + 1]
            : _data.Length;

        return _data.Slice(start, end - start);
    }

    public void Dispose()
    {
        var data = _data;
        _data = null;
        data?.Dispose();
    }
}
