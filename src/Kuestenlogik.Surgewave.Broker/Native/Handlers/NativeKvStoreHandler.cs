using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol KV Store and Object Store operations.
/// </summary>
public sealed class NativeKvStoreHandler : INativeRequestHandler
{
    private readonly KvBucketManager _bucketManager;
    private readonly ILogger<NativeKvStoreHandler> _logger;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        // KV Store
        SurgewaveOpCode.KvCreateBucket,
        SurgewaveOpCode.KvDeleteBucket,
        SurgewaveOpCode.KvListBuckets,
        SurgewaveOpCode.KvGet,
        SurgewaveOpCode.KvPut,
        SurgewaveOpCode.KvDelete,
        SurgewaveOpCode.KvListKeys,
        SurgewaveOpCode.KvHistory,
        SurgewaveOpCode.KvWatch,
        SurgewaveOpCode.KvPurge,

        // Object Store
        SurgewaveOpCode.ObjCreateStore,
        SurgewaveOpCode.ObjPutObject,
        SurgewaveOpCode.ObjGetObject,
        SurgewaveOpCode.ObjDeleteObject,
        SurgewaveOpCode.ObjListObjects,
        SurgewaveOpCode.ObjGetObjectInfo,
    ];

    public NativeKvStoreHandler(KvBucketManager bucketManager, ILogger<NativeKvStoreHandler> logger)
    {
        _bucketManager = bucketManager;
        _logger = logger;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.KvCreateBucket => HandleCreateBucketAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvDeleteBucket => HandleDeleteBucketAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvListBuckets => HandleListBucketsAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvGet => HandleGetAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvPut => HandlePutAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvDelete => HandleDeleteAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvListKeys => HandleListKeysAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvHistory => HandleHistoryAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvWatch => HandleWatchAsync(context, payload, cancellationToken),
            SurgewaveOpCode.KvPurge => HandlePurgeAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjCreateStore => HandleObjCreateStoreAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjPutObject => HandleObjPutObjectAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjGetObject => HandleObjGetObjectAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjDeleteObject => HandleObjDeleteObjectAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjListObjects => HandleObjListObjectsAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ObjGetObjectInfo => HandleObjGetObjectInfoAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask,
        };
    }

    // -----------------------------------------------------------------------
    // KV handlers
    // -----------------------------------------------------------------------

    private async Task HandleCreateBucketAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var name = reader.ReadString();

        if (string.IsNullOrEmpty(name))
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvCreateBucket,
                SurgewaveErrorCode.InvalidRequest, "Bucket name is required", cancellationToken);
            return;
        }

        var config = new KvBucketConfig();
        if (reader.Remaining >= 4)
            config.MaxHistoryPerKey = reader.ReadInt32();
        if (reader.Remaining >= 4)
            config.MaxValueSize = reader.ReadInt32();

        try
        {
            var bucket = await _bucketManager.CreateBucketAsync(name, config, cancellationToken);
            var info = bucket.GetInfo();

            // Response: [string name] [int32 keyCount] [int64 revision]
            var buffer = new byte[256];
            var writer = new SurgewavePayloadWriter(buffer);
            writer.WriteString(info.Name);
            writer.WriteInt32(info.KeyCount);
            writer.WriteInt64(info.LatestRevision);

            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvCreateBucket,
                SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvCreateBucket,
                SurgewaveErrorCode.KvBucketAlreadyExists, ex.Message, cancellationToken);
        }
    }

    private async Task HandleDeleteBucketAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var name = reader.ReadString();

        if (string.IsNullOrEmpty(name))
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvDeleteBucket,
                SurgewaveErrorCode.InvalidRequest, "Bucket name is required", cancellationToken);
            return;
        }

        var deleted = _bucketManager.DeleteBucket(name);
        if (!deleted)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvDeleteBucket,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{name}' not found", cancellationToken);
            return;
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvDeleteBucket,
            SurgewaveErrorCode.None, ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    private async Task HandleListBucketsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var names = _bucketManager.ListBuckets();

        // Response: [int32 count] [string name]*
        var buffer = new byte[4 + names.Sum(n => 2 + Encoding.UTF8.GetByteCount(n))];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(names.Count);
        foreach (var name in names)
            writer.WriteString(name);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvListBuckets,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleGetAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.ReadString();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvGet,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        var entry = bucket.Get(key!);
        if (entry is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvGet,
                SurgewaveErrorCode.KvKeyNotFound, $"Key '{key}' not found", cancellationToken);
            return;
        }

        await SendKvEntryResponseAsync(context, SurgewaveOpCode.KvGet, entry, cancellationToken);
    }

    private async Task HandlePutAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.ReadString();
        var value = reader.ReadBytes().ToArray();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvPut,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        try
        {
            var entry = await bucket.PutAsync(key!, value, cancellationToken);
            await SendKvEntryResponseAsync(context, SurgewaveOpCode.KvPut, entry, cancellationToken);
        }
        catch (ArgumentException ex)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvPut,
                SurgewaveErrorCode.InvalidRequest, ex.Message, cancellationToken);
        }
    }

    private async Task HandleDeleteAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.ReadString();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvDelete,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        var entry = await bucket.DeleteAsync(key!, cancellationToken);
        await SendKvEntryResponseAsync(context, SurgewaveOpCode.KvDelete, entry, cancellationToken);
    }

    private async Task HandleListKeysAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvListKeys,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        var keys = bucket.Keys();

        // Response: [int32 count] [string key]*
        var bufferSize = 4 + keys.Sum(k => 2 + Encoding.UTF8.GetByteCount(k));
        var buffer = new byte[bufferSize];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(keys.Count);
        foreach (var key in keys)
            writer.WriteString(key);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvListKeys,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleHistoryAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.ReadString();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvHistory,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        var history = bucket.History(key!);

        // Response: [int32 count] [entry]*
        var bufferSize = 4 + history.Sum(e => 2 + Encoding.UTF8.GetByteCount(e.Key) + 4 + e.Value.Length + 8 + 8 + 1);
        var buffer = new byte[bufferSize];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(history.Count);
        foreach (var entry in history)
        {
            WriteKvEntry(ref writer, entry);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvHistory,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleWatchAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.Remaining > 0 ? reader.ReadNullableString() : null;

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvWatch,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        // Create a watcher and send the subscription ID
        var (subscriptionId, _) = bucket.Watch(key);

        var buffer = new byte[16];
        var guidBytes = subscriptionId.ToByteArray();
        guidBytes.CopyTo(buffer.AsSpan());

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvWatch,
            SurgewaveErrorCode.None, buffer.AsMemory(0, 16), cancellationToken);
    }

    private async Task HandlePurgeAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var bucketName = reader.ReadString();
        var key = reader.ReadString();

        var bucket = _bucketManager.GetBucket(bucketName!);
        if (bucket is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.KvPurge,
                SurgewaveErrorCode.KvBucketNotFound, $"KV bucket '{bucketName}' not found", cancellationToken);
            return;
        }

        await bucket.PurgeAsync(key!, cancellationToken);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.KvPurge,
            SurgewaveErrorCode.None, ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    // -----------------------------------------------------------------------
    // Object Store handlers
    // -----------------------------------------------------------------------

    // Track object stores for native protocol (shared with REST API via bucket manager)
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ObjectStore> _objectStores = new();

    private async Task<ObjectStore?> GetOrCreateObjectStoreAsync(string storeName, bool createIfMissing, CancellationToken cancellationToken)
    {
        if (_objectStores.TryGetValue(storeName, out var existing))
            return existing;

        if (!createIfMissing)
            return null;

        var store = new ObjectStore(storeName, _bucketManager);
        if (_objectStores.TryAdd(storeName, store))
        {
            await store.EnsureCreatedAsync(cancellationToken);
            return store;
        }

        return _objectStores.GetValueOrDefault(storeName);
    }

    private async Task HandleObjCreateStoreAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();

        if (string.IsNullOrEmpty(storeName))
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjCreateStore,
                SurgewaveErrorCode.InvalidRequest, "Store name is required", cancellationToken);
            return;
        }

        var store = await GetOrCreateObjectStoreAsync(storeName, createIfMissing: true, cancellationToken);

        var buffer = new byte[256];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteString(storeName);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjCreateStore,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleObjPutObjectAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();
        var objectName = reader.ReadString();
        var contentType = reader.ReadNullableString();
        var data = reader.ReadBytes().ToArray();

        var store = await GetOrCreateObjectStoreAsync(storeName!, createIfMissing: false, cancellationToken);
        if (store is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjPutObject,
                SurgewaveErrorCode.ObjStoreNotFound, $"Object store '{storeName}' not found", cancellationToken);
            return;
        }

        var info = await store.PutObjectAsync(objectName!, data, contentType, cancellationToken);

        var buffer = new byte[512];
        var writer = new SurgewavePayloadWriter(buffer);
        WriteObjectInfo(ref writer, info);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjPutObject,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleObjGetObjectAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();
        var objectName = reader.ReadString();

        var store = await GetOrCreateObjectStoreAsync(storeName!, createIfMissing: false, cancellationToken);
        if (store is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObject,
                SurgewaveErrorCode.ObjStoreNotFound, $"Object store '{storeName}' not found", cancellationToken);
            return;
        }

        var result = store.GetObject(objectName!);
        if (result is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObject,
                SurgewaveErrorCode.ObjObjectNotFound, $"Object '{objectName}' not found", cancellationToken);
            return;
        }

        // Response: [ObjectInfo] [bytes data]
        var buffer = new byte[512 + result.Data.Length];
        var writer = new SurgewavePayloadWriter(buffer);
        WriteObjectInfo(ref writer, result.Info);
        writer.WriteBytes(result.Data);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObject,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleObjDeleteObjectAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();
        var objectName = reader.ReadString();

        var store = await GetOrCreateObjectStoreAsync(storeName!, createIfMissing: false, cancellationToken);
        if (store is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjDeleteObject,
                SurgewaveErrorCode.ObjStoreNotFound, $"Object store '{storeName}' not found", cancellationToken);
            return;
        }

        var deleted = await store.DeleteObjectAsync(objectName!, cancellationToken);
        if (!deleted)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjDeleteObject,
                SurgewaveErrorCode.ObjObjectNotFound, $"Object '{objectName}' not found", cancellationToken);
            return;
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjDeleteObject,
            SurgewaveErrorCode.None, ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    private async Task HandleObjListObjectsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();

        var store = await GetOrCreateObjectStoreAsync(storeName!, createIfMissing: false, cancellationToken);
        if (store is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjListObjects,
                SurgewaveErrorCode.ObjStoreNotFound, $"Object store '{storeName}' not found", cancellationToken);
            return;
        }

        var objects = store.ListObjects();

        var bufferSize = 4 + objects.Sum(o => 2 + Encoding.UTF8.GetByteCount(o.Name) + 8 + 4 + 2 + Encoding.UTF8.GetByteCount(o.ContentType ?? "") + 8 + 1);
        var buffer = new byte[bufferSize];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(objects.Count);
        foreach (var obj in objects)
        {
            WriteObjectInfo(ref writer, obj);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjListObjects,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private async Task HandleObjGetObjectInfoAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var storeName = reader.ReadString();
        var objectName = reader.ReadString();

        var store = await GetOrCreateObjectStoreAsync(storeName!, createIfMissing: false, cancellationToken);
        if (store is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObjectInfo,
                SurgewaveErrorCode.ObjStoreNotFound, $"Object store '{storeName}' not found", cancellationToken);
            return;
        }

        var info = store.GetObjectInfo(objectName!);
        if (info is null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObjectInfo,
                SurgewaveErrorCode.ObjObjectNotFound, $"Object '{objectName}' not found", cancellationToken);
            return;
        }

        var buffer = new byte[512];
        var writer = new SurgewavePayloadWriter(buffer);
        WriteObjectInfo(ref writer, info);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ObjGetObjectInfo,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    // -----------------------------------------------------------------------
    // Wire format helpers
    // -----------------------------------------------------------------------

    private async Task SendKvEntryResponseAsync(NativeRequestContext context, SurgewaveOpCode opCode, KvEntry entry, CancellationToken cancellationToken)
    {
        // Response: [string key] [bytes value] [int64 revision] [int64 created-ticks] [uint8 operation]
        var bufferSize = 2 + Encoding.UTF8.GetByteCount(entry.Key) + 4 + entry.Value.Length + 8 + 8 + 1;
        var buffer = new byte[bufferSize];
        var writer = new SurgewavePayloadWriter(buffer);
        WriteKvEntry(ref writer, entry);

        await context.SendResponseAsync(context.Header.RequestId, opCode,
            SurgewaveErrorCode.None, buffer.AsMemory(0, writer.Position), cancellationToken);
    }

    private static void WriteKvEntry(ref SurgewavePayloadWriter writer, KvEntry entry)
    {
        writer.WriteString(entry.Key);
        writer.WriteBytes(entry.Value);
        writer.WriteInt64(entry.Revision);
        writer.WriteInt64(entry.Created.UtcTicks);
        writer.WriteUInt8((byte)entry.Operation);
    }

    private static void WriteObjectInfo(ref SurgewavePayloadWriter writer, ObjectInfo info)
    {
        writer.WriteString(info.Name);
        writer.WriteInt64(info.Size);
        writer.WriteInt32(info.Chunks);
        writer.WriteNullableString(info.ContentType);
        writer.WriteInt64(info.Created.UtcTicks);
    }
}
