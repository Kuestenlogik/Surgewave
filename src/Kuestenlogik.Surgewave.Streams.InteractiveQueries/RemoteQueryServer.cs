using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// TCP server embedded in StreamsApplication for serving Remote Interactive Queries.
/// Handles key-value Get, Range, All, Count, and metadata requests.
/// </summary>
internal sealed class RemoteQueryServer : IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly HostInfo _hostInfo;
    private readonly Func<string, IStateStore?> _storeResolver;
    private readonly Func<StreamsMetadata> _metadataProvider;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private TcpListener? _listener;
    private Task? _acceptTask;

    public RemoteQueryServer(
        HostInfo hostInfo,
        Func<string, IStateStore?> storeResolver,
        Func<StreamsMetadata> metadataProvider,
        ILogger logger)
    {
        _hostInfo = hostInfo;
        _storeResolver = storeResolver;
        _metadataProvider = metadataProvider;
        _logger = logger;
    }

    public void Start()
    {
        _listener = new TcpListener(IPAddress.Parse(_hostInfo.Host == "localhost" ? "0.0.0.0" : _hostInfo.Host), _hostInfo.Port);
        _listener.Start();
        _acceptTask = Task.Run(AcceptLoop);
        _logger.LogInformation("Remote query server started on {Host}:{Port}", _hostInfo.Host, _hostInfo.Port);
    }

    private async Task AcceptLoop()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var client = await _listener!.AcceptTcpClientAsync(_cts.Token);
                _ = Task.Run(() => HandleClient(client));
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in query server accept loop");
        }
    }

    private async Task HandleClient(TcpClient client)
    {
        try
        {
            using (client)
            {
                client.NoDelay = true;
                using var stream = client.GetStream();
                using var reader = new BinaryReader(stream);
                using var writer = new BinaryWriter(stream);

                // Read request: [1 byte: type][payload]
                var requestType = reader.ReadByte();

                switch (requestType)
                {
                    case QueryProtocol.MetadataRequest:
                        HandleMetadataRequest(writer);
                        break;
                    case QueryProtocol.KeyValueGetRequest:
                        HandleKeyValueGet(reader, writer);
                        break;
                    case QueryProtocol.KeyValueRangeRequest:
                        HandleKeyValueRange(reader, writer);
                        break;
                    case QueryProtocol.KeyValueAllRequest:
                        HandleKeyValueAll(reader, writer);
                        break;
                    case QueryProtocol.KeyValueCountRequest:
                        HandleKeyValueCount(reader, writer);
                        break;
                    default:
                        WriteError(writer, $"Unknown request type: 0x{requestType:X2}");
                        break;
                }

                await writer.BaseStream.FlushAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error handling query client");
        }
    }

    private void HandleMetadataRequest(BinaryWriter writer)
    {
        var metadata = _metadataProvider();
        writer.Write(QueryProtocol.MetadataResponse);
        writer.Write(QueryProtocol.StatusOk);

        // Serialize metadata as JSON
        var json = JsonSerializer.Serialize(new MetadataDto
        {
            Host = metadata.HostInfo.Host,
            Port = metadata.HostInfo.Port,
            StoreNames = [.. metadata.StateStoreNames],
            Partitions = metadata.TopicPartitions
                .Select(tp => new PartitionDto { Topic = tp.Topic, Partition = tp.Partition })
                .ToList()
        });
        QueryProtocol.WriteString(writer, json);
    }

    private void HandleKeyValueGet(BinaryReader reader, BinaryWriter writer)
    {
        var storeName = QueryProtocol.ReadString(reader);
        var keyBytes = QueryProtocol.ReadBytes(reader);

        var store = _storeResolver(storeName);
        if (store == null)
        {
            writer.Write(QueryProtocol.KeyValueGetResponse);
            writer.Write(QueryProtocol.StatusStoreNotFound);
            return;
        }

        // Use reflection to call Get with raw bytes
        var valueBytes = GetRawValue(store, keyBytes);
        if (valueBytes == null)
        {
            writer.Write(QueryProtocol.KeyValueGetResponse);
            writer.Write(QueryProtocol.StatusNotFound);
            return;
        }

        writer.Write(QueryProtocol.KeyValueGetResponse);
        writer.Write(QueryProtocol.StatusOk);
        QueryProtocol.WriteBytes(writer, valueBytes);
    }

    private void HandleKeyValueRange(BinaryReader reader, BinaryWriter writer)
    {
        var storeName = QueryProtocol.ReadString(reader);
        var fromBytes = QueryProtocol.ReadBytes(reader);
        var toBytes = QueryProtocol.ReadBytes(reader);

        var store = _storeResolver(storeName);
        if (store == null)
        {
            writer.Write(QueryProtocol.KeyValueRangeResponse);
            writer.Write(QueryProtocol.StatusStoreNotFound);
            return;
        }

        var entries = GetRawRange(store, fromBytes, toBytes);
        writer.Write(QueryProtocol.KeyValueRangeResponse);
        writer.Write(QueryProtocol.StatusOk);
        WriteEntries(writer, entries);
    }

    private void HandleKeyValueAll(BinaryReader reader, BinaryWriter writer)
    {
        var storeName = QueryProtocol.ReadString(reader);

        var store = _storeResolver(storeName);
        if (store == null)
        {
            writer.Write(QueryProtocol.KeyValueAllResponse);
            writer.Write(QueryProtocol.StatusStoreNotFound);
            return;
        }

        var entries = GetRawAll(store);
        writer.Write(QueryProtocol.KeyValueAllResponse);
        writer.Write(QueryProtocol.StatusOk);
        WriteEntries(writer, entries);
    }

    private void HandleKeyValueCount(BinaryReader reader, BinaryWriter writer)
    {
        var storeName = QueryProtocol.ReadString(reader);

        var store = _storeResolver(storeName);
        if (store == null)
        {
            writer.Write(QueryProtocol.KeyValueCountResponse);
            writer.Write(QueryProtocol.StatusStoreNotFound);
            return;
        }

        var count = GetApproximateCount(store);
        writer.Write(QueryProtocol.KeyValueCountResponse);
        writer.Write(QueryProtocol.StatusOk);
        writer.Write(count);
    }

    private static void WriteError(BinaryWriter writer, string message)
    {
        writer.Write(QueryProtocol.ErrorResponse);
        writer.Write(QueryProtocol.StatusError);
        QueryProtocol.WriteString(writer, message);
    }

    private static void WriteEntries(BinaryWriter writer, List<(byte[] key, byte[] value)> entries)
    {
        writer.Write(entries.Count);
        foreach (var (key, value) in entries)
        {
            QueryProtocol.WriteBytes(writer, key);
            QueryProtocol.WriteBytes(writer, value);
        }
    }

    // --- Raw byte access to stores via reflection ---
    // State stores work with typed keys/values. For remote queries, we need
    // to access the raw serialized bytes. We use the stores' internal ISerde
    // instances via the IByteAccessibleStore interface.

    private static byte[]? GetRawValue(IStateStore store, byte[] keyBytes)
    {
        if (store is IRawByteStore rawStore)
            return rawStore.GetRaw(keyBytes);

        var storeType = store.GetType();
        var kvInterface = storeType.GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyKeyValueStore<,>));
        if (kvInterface == null) return null;

        var genericArgs = kvInterface.GetGenericArguments();
        var getMethod = kvInterface.GetMethod("Get");
        if (getMethod == null) return null;

        // Try to get serdes from the store (persistent stores have them)
        var keySerde = GetFieldValue(storeType, store, "_keySerde");
        var valueSerde = GetFieldValue(storeType, store, "_valueSerde");

        object? key;
        if (keySerde != null)
        {
            var deserializeKey = keySerde.GetType().GetMethod("Deserialize");
            key = deserializeKey?.Invoke(keySerde, [keyBytes]);
        }
        else
        {
            // Fallback: deserialize key as JSON (for InMemoryKeyValueStore etc.)
            key = JsonSerializer.Deserialize(keyBytes, genericArgs[0], JsonOptions);
        }

        if (key == null) return null;

        var value = getMethod.Invoke(store, [key]);
        if (value == null) return null;

        if (valueSerde != null)
        {
            var serializeValue = valueSerde.GetType().GetMethod("Serialize");
            return serializeValue?.Invoke(valueSerde, [value]) as byte[];
        }

        // Fallback: serialize value as JSON
        return JsonSerializer.SerializeToUtf8Bytes(value, genericArgs[1], JsonOptions);
    }

    private static List<(byte[] key, byte[] value)> GetRawRange(IStateStore store, byte[] fromBytes, byte[] toBytes)
    {
        return GetRawEntries(store, "Range", fromBytes, toBytes);
    }

    private static List<(byte[] key, byte[] value)> GetRawAll(IStateStore store)
    {
        return GetRawEntries(store, "All", null, null);
    }

    private static List<(byte[] key, byte[] value)> GetRawEntries(IStateStore store, string methodName, byte[]? fromBytes, byte[]? toBytes)
    {
        var result = new List<(byte[] key, byte[] value)>();

        var storeType = store.GetType();
        var kvInterface = storeType.GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyKeyValueStore<,>));
        if (kvInterface == null) return result;

        var genericArgs = kvInterface.GetGenericArguments();
        var keySerde = GetFieldValue(storeType, store, "_keySerde");
        var valueSerde = GetFieldValue(storeType, store, "_valueSerde");

        object? entries;
        if (methodName == "All")
        {
            var allMethod = kvInterface.GetMethod("All");
            entries = allMethod?.Invoke(store, null);
        }
        else
        {
            object? from, to;
            if (keySerde != null)
            {
                var deserializeKey = keySerde.GetType().GetMethod("Deserialize");
                from = deserializeKey?.Invoke(keySerde, [fromBytes!]);
                to = deserializeKey?.Invoke(keySerde, [toBytes!]);
            }
            else
            {
                from = JsonSerializer.Deserialize(fromBytes!, genericArgs[0], JsonOptions);
                to = JsonSerializer.Deserialize(toBytes!, genericArgs[0], JsonOptions);
            }

            var rangeMethod = kvInterface.GetMethod("Range");
            entries = rangeMethod?.Invoke(store, [from, to]);
        }

        if (entries == null) return result;

        foreach (var entry in (System.Collections.IEnumerable)entries)
        {
            var entryType = entry.GetType();
            var keyProp = entryType.GetProperty("Key");
            var valueProp = entryType.GetProperty("Value");
            if (keyProp == null || valueProp == null) continue;

            var key = keyProp.GetValue(entry);
            var value = valueProp.GetValue(entry);
            if (key == null || value == null) continue;

            byte[]? kBytes, vBytes;
            if (keySerde != null && valueSerde != null)
            {
                var serializeKey = keySerde.GetType().GetMethod("Serialize");
                var serializeValue = valueSerde.GetType().GetMethod("Serialize");
                kBytes = serializeKey?.Invoke(keySerde, [key]) as byte[];
                vBytes = serializeValue?.Invoke(valueSerde, [value]) as byte[];
            }
            else
            {
                kBytes = JsonSerializer.SerializeToUtf8Bytes(key, genericArgs[0], JsonOptions);
                vBytes = JsonSerializer.SerializeToUtf8Bytes(value, genericArgs[1], JsonOptions);
            }

            if (kBytes != null && vBytes != null)
                result.Add((kBytes, vBytes));
        }

        return result;
    }

    private static object? GetFieldValue(Type storeType, object store, string fieldName)
    {
        var field = storeType.GetField(fieldName,
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return field?.GetValue(store);
    }

    private static long GetApproximateCount(IStateStore store)
    {
        var prop = store.GetType().GetProperty("ApproximateNumEntries");
        if (prop?.GetValue(store) is long count)
            return count;
        return 0;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener?.Stop();

        if (_acceptTask != null)
        {
            try { await _acceptTask; } catch { }
        }

        _listener?.Dispose();
        _cts.Dispose();
    }

    // DTOs for JSON metadata serialization
    internal sealed class MetadataDto
    {
        public string Host { get; init; } = "";
        public int Port { get; init; }
        public List<string> StoreNames { get; init; } = [];
        public List<PartitionDto> Partitions { get; init; } = [];
    }

    internal sealed class PartitionDto
    {
        public string Topic { get; init; } = "";
        public int Partition { get; init; }
    }
}
