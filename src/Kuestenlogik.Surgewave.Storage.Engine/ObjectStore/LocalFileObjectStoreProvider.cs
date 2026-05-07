namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Local filesystem implementation of IObjectStoreProvider.
/// Useful for development, testing, and single-node deployments.
/// Stores segments as files organized by topic/partition/offset.
/// </summary>
public sealed class LocalFileObjectStoreProvider : IObjectStoreProvider
{
    private readonly string _basePath;

    public LocalFileObjectStoreProvider(string basePath)
    {
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
    }

    public async Task UploadAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        var dir = GetSegmentDirectory(topic, partition);
        Directory.CreateDirectory(dir);

        var filePath = GetSegmentPath(topic, partition, baseOffset);
        await File.WriteAllBytesAsync(filePath, data.ToArray(), cancellationToken);
    }

    public async Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetSegmentPath(topic, partition, baseOffset);

        if (!File.Exists(filePath))
            return null;

        return await File.ReadAllBytesAsync(filePath, cancellationToken);
    }

    public Task DeleteAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetSegmentPath(topic, partition, baseOffset);

        if (File.Exists(filePath))
            File.Delete(filePath);

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var dir = GetSegmentDirectory(topic, partition);

        if (!Directory.Exists(dir))
            return Task.FromResult<IReadOnlyList<long>>(Array.Empty<long>());

        var offsets = Directory.GetFiles(dir, "*.segment")
            .Select(f => Path.GetFileNameWithoutExtension(f))
            .Where(name => long.TryParse(name, out _))
            .Select(long.Parse)
            .Order()
            .ToList();

        return Task.FromResult<IReadOnlyList<long>>(offsets);
    }

    private string GetSegmentDirectory(string topic, int partition) =>
        Path.Combine(_basePath, topic, partition.ToString());

    private string GetSegmentPath(string topic, int partition, long baseOffset) =>
        Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.segment");
}
