using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol metadata operations: Ping, GetMetadata.
/// </summary>
public sealed class NativeMetadataHandler : INativeRequestHandler
{
    private readonly LogManager _logManager;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.Ping,
        SurgewaveOpCode.GetMetadata
    ];

    public NativeMetadataHandler(LogManager logManager)
    {
        _logManager = logManager;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.Ping => HandlePingAsync(context, cancellationToken),
            SurgewaveOpCode.GetMetadata => HandleGetMetadataAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandlePingAsync(NativeRequestContext context, CancellationToken cancellationToken)
    {
        var payload = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(payload, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.Pong,
            SurgewaveErrorCode.None, payload, cancellationToken);
    }

    private async Task HandleGetMetadataAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var topicCount = reader.ReadInt16();

        using var writer = new BigEndianWriter();

        List<TopicMetadata> topics;
        if (topicCount == 0)
        {
            topics = _logManager.ListTopics().ToList();
        }
        else
        {
            topics = new List<TopicMetadata>();
            for (int i = 0; i < topicCount; i++)
            {
                var name = reader.ReadString();
                if (name != null)
                {
                    var metadata = _logManager.GetTopicMetadata(name);
                    if (metadata != null)
                    {
                        topics.Add(metadata);
                    }
                }
            }
        }

        // Write broker info
        writer.Write((short)1);
        writer.Write(context.Config.BrokerId);
        writer.WriteString(context.Config.Host);
        writer.Write(context.Config.Port);

        // Write topic metadata
        writer.Write((short)topics.Count);
        foreach (var topic in topics)
        {
            writer.Write((short)0); // error code
            writer.WriteString(topic.Name);
            writer.Write(topic.PartitionCount);

            for (int i = 0; i < topic.PartitionCount; i++)
            {
                writer.Write((short)0); // partition error
                writer.Write(i); // partition id
                writer.Write(context.Config.BrokerId); // leader id
            }
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.GetMetadata,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }
}
