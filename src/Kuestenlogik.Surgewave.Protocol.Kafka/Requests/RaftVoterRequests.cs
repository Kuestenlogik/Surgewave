namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

// ────────────────────────────────────────────────────────────────
// AddRaftVoter (API Key 80)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka AddRaftVoter request (API Key 80, v0-1).
/// KIP-853: Dynamic Raft voter reconfiguration — add a voter to the KRaft cluster.
/// </summary>
public sealed class AddRaftVoterRequest : KafkaRequest
{
    /// <summary>The cluster ID.</summary>
    public string? ClusterId { get; init; }

    /// <summary>The maximum time to wait for the request to complete before returning.</summary>
    public int TimeoutMs { get; init; }

    /// <summary>The replica ID of the voter getting added to the topic partition.</summary>
    public int VoterId { get; init; }

    /// <summary>The directory ID of the voter getting added to the topic partition.</summary>
    public Guid VoterDirectoryId { get; init; }

    /// <summary>The endpoints that can be used to communicate with the voter.</summary>
    public required List<ListenerInfo> Listeners { get; init; }

    /// <summary>When true, return a response after the new voter set is committed (v1+).</summary>
    public bool AckWhenCommitted { get; init; } = true;

    public sealed class ListenerInfo
    {
        /// <summary>The name of the endpoint.</summary>
        public required string Name { get; init; }

        /// <summary>The hostname.</summary>
        public required string Host { get; init; }

        /// <summary>The port.</summary>
        public ushort Port { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(ClusterId);
        writer.WriteInt32(TimeoutMs);
        writer.WriteInt32(VoterId);
        writer.WriteUuid(VoterDirectoryId);

        writer.WriteVarInt(Listeners.Count + 1);
        foreach (var listener in Listeners)
        {
            writer.WriteCompactString(listener.Name);
            writer.WriteCompactString(listener.Host);
            writer.WriteInt16((short)listener.Port);
            writer.WriteVarInt(0); // Listener tagged fields
        }

        if (ApiVersion >= 1)
        {
            writer.WriteBoolean(AckWhenCommitted);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AddRaftVoterRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clusterId = reader.ReadCompactString();
        var timeoutMs = reader.ReadInt32();
        var voterId = reader.ReadInt32();
        var voterDirectoryId = reader.ReadUuid();

        var listenerCount = reader.ReadVarInt() - 1;
        var listeners = new List<ListenerInfo>(listenerCount);
        for (int i = 0; i < listenerCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var host = reader.ReadCompactString() ?? "";
            var port = (ushort)reader.ReadInt16();
            reader.SkipTaggedFields();

            listeners.Add(new ListenerInfo
            {
                Name = name,
                Host = host,
                Port = port
            });
        }

        var ackWhenCommitted = true;
        if (apiVersion >= 1)
        {
            ackWhenCommitted = reader.ReadBoolean();
        }

        reader.SkipTaggedFields();

        return new AddRaftVoterRequest
        {
            ApiKey = ApiKey.AddRaftVoter,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            TimeoutMs = timeoutMs,
            VoterId = voterId,
            VoterDirectoryId = voterDirectoryId,
            Listeners = listeners,
            AckWhenCommitted = ackWhenCommitted
        };
    }
}

/// <summary>
/// Kafka AddRaftVoter response (API Key 80, v0-1).
/// </summary>
public sealed class AddRaftVoterResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AddRaftVoterResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        reader.SkipTaggedFields();

        return new AddRaftVoterResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}

// ────────────────────────────────────────────────────────────────
// RemoveRaftVoter (API Key 81)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka RemoveRaftVoter request (API Key 81, v0-0).
/// KIP-853: Dynamic Raft voter reconfiguration — remove a voter from the KRaft cluster.
/// </summary>
public sealed class RemoveRaftVoterRequest : KafkaRequest
{
    /// <summary>The cluster ID.</summary>
    public string? ClusterId { get; init; }

    /// <summary>The replica ID of the voter getting removed from the topic partition.</summary>
    public int VoterId { get; init; }

    /// <summary>The directory ID of the voter getting removed from the topic partition.</summary>
    public Guid VoterDirectoryId { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(ClusterId);
        writer.WriteInt32(VoterId);
        writer.WriteUuid(VoterDirectoryId);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static RemoveRaftVoterRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clusterId = reader.ReadCompactString();
        var voterId = reader.ReadInt32();
        var voterDirectoryId = reader.ReadUuid();

        reader.SkipTaggedFields();

        return new RemoveRaftVoterRequest
        {
            ApiKey = ApiKey.RemoveRaftVoter,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            VoterId = voterId,
            VoterDirectoryId = voterDirectoryId
        };
    }
}

/// <summary>
/// Kafka RemoveRaftVoter response (API Key 81, v0-0).
/// </summary>
public sealed class RemoveRaftVoterResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static RemoveRaftVoterResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        reader.SkipTaggedFields();

        return new RemoveRaftVoterResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}

// ────────────────────────────────────────────────────────────────
// UpdateRaftVoter (API Key 82)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka UpdateRaftVoter request (API Key 82, v0-0).
/// KIP-853: Dynamic Raft voter reconfiguration — update a voter in the KRaft cluster.
/// </summary>
public sealed class UpdateRaftVoterRequest : KafkaRequest
{
    /// <summary>The cluster ID.</summary>
    public string? ClusterId { get; init; }

    /// <summary>The current leader epoch of the partition, -1 for unknown leader epoch.</summary>
    public int CurrentLeaderEpoch { get; init; }

    /// <summary>The replica ID of the voter getting updated in the topic partition.</summary>
    public int VoterId { get; init; }

    /// <summary>The directory ID of the voter getting updated in the topic partition.</summary>
    public Guid VoterDirectoryId { get; init; }

    /// <summary>The endpoints that can be used to communicate with the leader.</summary>
    public required List<ListenerInfo> Listeners { get; init; }

    /// <summary>The range of versions of the KRaft protocol that the replica supports.</summary>
    public required KRaftVersionFeatureInfo KRaftVersionFeature { get; init; }

    public sealed class ListenerInfo
    {
        /// <summary>The name of the endpoint.</summary>
        public required string Name { get; init; }

        /// <summary>The hostname.</summary>
        public required string Host { get; init; }

        /// <summary>The port.</summary>
        public ushort Port { get; init; }
    }

    public sealed class KRaftVersionFeatureInfo
    {
        /// <summary>The minimum supported KRaft protocol version.</summary>
        public short MinSupportedVersion { get; init; }

        /// <summary>The maximum supported KRaft protocol version.</summary>
        public short MaxSupportedVersion { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(ClusterId);
        writer.WriteInt32(CurrentLeaderEpoch);
        writer.WriteInt32(VoterId);
        writer.WriteUuid(VoterDirectoryId);

        writer.WriteVarInt(Listeners.Count + 1);
        foreach (var listener in Listeners)
        {
            writer.WriteCompactString(listener.Name);
            writer.WriteCompactString(listener.Host);
            writer.WriteInt16((short)listener.Port);
            writer.WriteVarInt(0); // Listener tagged fields
        }

        writer.WriteInt16(KRaftVersionFeature.MinSupportedVersion);
        writer.WriteInt16(KRaftVersionFeature.MaxSupportedVersion);
        writer.WriteVarInt(0); // KRaftVersionFeature tagged fields

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static UpdateRaftVoterRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clusterId = reader.ReadCompactString();
        var currentLeaderEpoch = reader.ReadInt32();
        var voterId = reader.ReadInt32();
        var voterDirectoryId = reader.ReadUuid();

        var listenerCount = reader.ReadVarInt() - 1;
        var listeners = new List<ListenerInfo>(listenerCount);
        for (int i = 0; i < listenerCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var host = reader.ReadCompactString() ?? "";
            var port = (ushort)reader.ReadInt16();
            reader.SkipTaggedFields();

            listeners.Add(new ListenerInfo
            {
                Name = name,
                Host = host,
                Port = port
            });
        }

        var minSupportedVersion = reader.ReadInt16();
        var maxSupportedVersion = reader.ReadInt16();
        reader.SkipTaggedFields(); // KRaftVersionFeature tagged fields

        reader.SkipTaggedFields();

        return new UpdateRaftVoterRequest
        {
            ApiKey = ApiKey.UpdateRaftVoter,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            CurrentLeaderEpoch = currentLeaderEpoch,
            VoterId = voterId,
            VoterDirectoryId = voterDirectoryId,
            Listeners = listeners,
            KRaftVersionFeature = new KRaftVersionFeatureInfo
            {
                MinSupportedVersion = minSupportedVersion,
                MaxSupportedVersion = maxSupportedVersion
            }
        };
    }
}

/// <summary>
/// Kafka UpdateRaftVoter response (API Key 82, v0-0).
/// The CurrentLeader field is a tagged field (tag 0).
/// </summary>
public sealed class UpdateRaftVoterResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>Details of the current Raft cluster leader (tagged field, tag 0).</summary>
    public CurrentLeaderInfo? CurrentLeader { get; init; }

    public sealed class CurrentLeaderInfo
    {
        /// <summary>The replica ID of the current leader or -1 if the leader is unknown.</summary>
        public int LeaderId { get; init; } = -1;

        /// <summary>The latest known leader epoch.</summary>
        public int LeaderEpoch { get; init; } = -1;

        /// <summary>The node's hostname.</summary>
        public required string Host { get; init; }

        /// <summary>The node's port.</summary>
        public int Port { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        // CurrentLeader is a tagged field at tag 0
        if (CurrentLeader != null)
        {
            // Write 1 tagged field
            writer.WriteVarInt(1);
            writer.WriteVarInt(0); // Tag 0
            // Write the tagged field data with length prefix
            using var tagWriter = new KafkaProtocolWriter();
            tagWriter.WriteInt32(CurrentLeader.LeaderId);
            tagWriter.WriteInt32(CurrentLeader.LeaderEpoch);
            tagWriter.WriteCompactString(CurrentLeader.Host);
            tagWriter.WriteInt32(CurrentLeader.Port);
            tagWriter.WriteVarInt(0); // CurrentLeader tagged fields
            var tagData = tagWriter.ToArray();
            writer.WriteVarInt(tagData.Length);
            writer.WriteRawBytes(tagData);
        }
        else
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static UpdateRaftVoterResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        // The CurrentLeader is in tagged fields — for simplicity, skip tagged fields
        // A full implementation would parse tag 0 to extract CurrentLeader
        reader.SkipTaggedFields();

        return new UpdateRaftVoterResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode
        };
    }
}
