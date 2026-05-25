namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DeleteTopics request (API Key 20)
/// Used by admin clients to delete topics.
/// </summary>
public sealed class DeleteTopicsRequest : KafkaRequest
{
    public required List<string> TopicNames { get; init; }
    public required int TimeoutMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(TopicNames.Count);
        foreach (var topic in TopicNames)
        {
            writer.WriteString(topic);
        }
        writer.WriteInt32(TimeoutMs);
    }

    public static DeleteTopicsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 4;
        var topicNames = new List<string>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            // v6+ has TopicNames as nullable and adds Topics array with TopicId
            if (apiVersion >= 6)
            {
                // Topics array (COMPACT_ARRAY)
                var topicCount = protocolReader.ReadVarInt() - 1;
                for (int i = 0; i < topicCount; i++)
                {
                    var name = protocolReader.ReadCompactString();
                    // Skip TopicId (16 bytes UUID) - we use name
                    protocolReader.Skip(16);
                    protocolReader.ReadVarInt(); // tagged fields
                    if (name != null)
                    {
                        topicNames.Add(name);
                    }
                }
            }
            else
            {
                // TopicNames (COMPACT_ARRAY)
                var topicCount = protocolReader.ReadVarInt() - 1;
                for (int i = 0; i < topicCount; i++)
                {
                    var name = protocolReader.ReadCompactString();
                    if (name != null)
                    {
                        topicNames.Add(name);
                    }
                }
            }

            var timeoutMs = protocolReader.ReadInt32();
            protocolReader.ReadVarInt(); // tagged fields

            return new DeleteTopicsRequest
            {
                ApiKey = ApiKey.DeleteTopics,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                TopicNames = topicNames,
                TimeoutMs = timeoutMs
            };
        }
        else
        {
            // Non-flexible format (v0-v3)
            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                topicNames.Add(BinaryHelpers.ReadString(reader));
            }
            var timeoutMs = BinaryHelpers.ReadInt32BigEndian(reader);

            return new DeleteTopicsRequest
            {
                ApiKey = ApiKey.DeleteTopics,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                TopicNames = topicNames,
                TimeoutMs = timeoutMs
            };
        }
    }
}

/// <summary>
/// Kafka DeleteTopics response
/// </summary>
public sealed class DeleteTopicsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; } // v1+
    public required List<TopicResult> Responses { get; init; }

    public sealed class TopicResult
    {
        public required string Name { get; init; }
        public required ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; } // v5+
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 4;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response header tagged fields
        }

        // ThrottleTimeMs (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(Responses.Count + 1); // COMPACT_ARRAY
            foreach (var result in Responses)
            {
                writer.WriteCompactString(result.Name);
                // TopicId (v6+, 16 bytes UUID)
                if (ApiVersion >= 6)
                {
                    writer.WriteRawBytes(Guid.Empty.ToByteArray());
                }
                writer.WriteInt16((short)result.ErrorCode);
                // ErrorMessage (v5+)
                if (ApiVersion >= 5)
                {
                    writer.WriteCompactString(result.ErrorMessage);
                }
                writer.WriteVarInt(0); // tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Responses.Count);
            foreach (var result in Responses)
            {
                writer.WriteString(result.Name);
                writer.WriteInt16((short)result.ErrorCode);
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
