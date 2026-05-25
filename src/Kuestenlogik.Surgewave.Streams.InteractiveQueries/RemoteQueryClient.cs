using System.Net.Sockets;
using System.Text.Json;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// TCP client for querying remote Streams application instances.
/// Each call opens a new connection (simple, stateless, no connection pooling in v1).
/// </summary>
public sealed class RemoteQueryClient : IDisposable
{
    private readonly HostInfo _target;
    private readonly TimeSpan _timeout;

    public RemoteQueryClient(HostInfo target, TimeSpan? timeout = null)
    {
        _target = target;
        _timeout = timeout ?? TimeSpan.FromSeconds(10);
    }

    /// <summary>
    /// Get metadata from the remote instance.
    /// </summary>
    public async Task<StreamsMetadata?> GetMetadataAsync(CancellationToken ct = default)
    {
        using var client = await ConnectAsync(ct);
        using var stream = client.GetStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);

        writer.Write(QueryProtocol.MetadataRequest);
        await writer.BaseStream.FlushAsync(ct);

        var responseType = reader.ReadByte();
        var status = reader.ReadByte();

        if (responseType != QueryProtocol.MetadataResponse || status != QueryProtocol.StatusOk)
            return null;

        var json = QueryProtocol.ReadString(reader);
        var dto = JsonSerializer.Deserialize<RemoteQueryServer.MetadataDto>(json);
        if (dto == null) return null;

        return new StreamsMetadata(
            new HostInfo(dto.Host, dto.Port),
            dto.StoreNames,
            dto.Partitions.Select(p => new TopicPartition(p.Topic, p.Partition)));
    }

    /// <summary>
    /// Get a value by key from a remote state store (raw bytes).
    /// </summary>
    public async Task<byte[]?> GetRawAsync(string storeName, byte[] keyBytes, CancellationToken ct = default)
    {
        using var client = await ConnectAsync(ct);
        using var stream = client.GetStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);

        writer.Write(QueryProtocol.KeyValueGetRequest);
        QueryProtocol.WriteString(writer, storeName);
        QueryProtocol.WriteBytes(writer, keyBytes);
        await writer.BaseStream.FlushAsync(ct);

        var responseType = reader.ReadByte();
        var status = reader.ReadByte();

        if (responseType != QueryProtocol.KeyValueGetResponse)
            return null;

        return status switch
        {
            QueryProtocol.StatusOk => QueryProtocol.ReadBytes(reader),
            _ => null
        };
    }

    /// <summary>
    /// Get a typed value from a remote state store.
    /// </summary>
    public async Task<TValue?> GetAsync<TKey, TValue>(
        string storeName, TKey key, ISerde<TKey> keySerde, ISerde<TValue> valueSerde,
        CancellationToken ct = default)
        where TKey : notnull
    {
        var keyBytes = keySerde.Serialize(key);
        var valueBytes = await GetRawAsync(storeName, keyBytes, ct);
        if (valueBytes == null) return default;
        return valueSerde.Deserialize(valueBytes);
    }

    /// <summary>
    /// Get a range of entries from a remote state store (raw bytes).
    /// </summary>
    public async Task<List<(byte[] key, byte[] value)>> RangeRawAsync(
        string storeName, byte[] fromBytes, byte[] toBytes, CancellationToken ct = default)
    {
        using var client = await ConnectAsync(ct);
        using var stream = client.GetStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);

        writer.Write(QueryProtocol.KeyValueRangeRequest);
        QueryProtocol.WriteString(writer, storeName);
        QueryProtocol.WriteBytes(writer, fromBytes);
        QueryProtocol.WriteBytes(writer, toBytes);
        await writer.BaseStream.FlushAsync(ct);

        return ReadEntriesResponse(reader, QueryProtocol.KeyValueRangeResponse);
    }

    /// <summary>
    /// Get all entries from a remote state store (raw bytes).
    /// </summary>
    public async Task<List<(byte[] key, byte[] value)>> AllRawAsync(
        string storeName, CancellationToken ct = default)
    {
        using var client = await ConnectAsync(ct);
        using var stream = client.GetStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);

        writer.Write(QueryProtocol.KeyValueAllRequest);
        QueryProtocol.WriteString(writer, storeName);
        await writer.BaseStream.FlushAsync(ct);

        return ReadEntriesResponse(reader, QueryProtocol.KeyValueAllResponse);
    }

    /// <summary>
    /// Get the approximate entry count from a remote state store.
    /// </summary>
    public async Task<long> CountAsync(string storeName, CancellationToken ct = default)
    {
        using var client = await ConnectAsync(ct);
        using var stream = client.GetStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);

        writer.Write(QueryProtocol.KeyValueCountRequest);
        QueryProtocol.WriteString(writer, storeName);
        await writer.BaseStream.FlushAsync(ct);

        var responseType = reader.ReadByte();
        var status = reader.ReadByte();

        if (responseType == QueryProtocol.KeyValueCountResponse && status == QueryProtocol.StatusOk)
            return reader.ReadInt64();

        return 0;
    }

    private static List<(byte[] key, byte[] value)> ReadEntriesResponse(BinaryReader reader, byte expectedType)
    {
        var responseType = reader.ReadByte();
        var status = reader.ReadByte();
        var result = new List<(byte[] key, byte[] value)>();

        if (responseType != expectedType || status != QueryProtocol.StatusOk)
            return result;

        var count = reader.ReadInt32();
        for (var i = 0; i < count; i++)
        {
            var key = QueryProtocol.ReadBytes(reader);
            var value = QueryProtocol.ReadBytes(reader);
            result.Add((key, value));
        }

        return result;
    }

    private async Task<TcpClient> ConnectAsync(CancellationToken ct)
    {
        var client = new TcpClient { NoDelay = true };
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_timeout);

        await client.ConnectAsync(_target.Host, _target.Port, timeoutCts.Token);
        return client;
    }

    public void Dispose() { }
}
