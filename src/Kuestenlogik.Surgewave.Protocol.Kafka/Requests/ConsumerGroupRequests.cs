namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ConsumerGroupHeartbeat request (API Key 68, v0-0).
/// KIP-848: Next generation consumer group protocol heartbeat.
/// </summary>
public sealed class ConsumerGroupHeartbeatRequest : KafkaRequest
{
    /// <summary>The group ID string.</summary>
    public required string GroupId { get; init; }

    /// <summary>The member ID.</summary>
    public required string MemberId { get; init; }

    /// <summary>The member epoch.</summary>
    public int MemberEpoch { get; init; }

    /// <summary>
    /// The unique instance ID of a static member.
    /// </summary>
    public string? InstanceId { get; init; }

    /// <summary>
    /// The rack ID of the consumer.
    /// </summary>
    public string? RackId { get; init; }

    /// <summary>
    /// The maximum time in ms that the coordinator will wait for each member to heartbeat.
    /// -1 means the group's configured value.
    /// </summary>
    public int RebalanceTimeoutMs { get; init; } = -1;

    /// <summary>
    /// The list of subscribed topics. Null if unchanged since last heartbeat.
    /// </summary>
    public List<string>? SubscribedTopicNames { get; init; }

    /// <summary>
    /// The server assignor to use. Null means the group's configured value.
    /// </summary>
    public string? ServerAssignor { get; init; }

    /// <summary>
    /// The client-side assignors. Null means no change since last heartbeat.
    /// </summary>
    public List<Assignor>? TopicPartitions { get; init; }

    /// <summary>
    /// v1+ KIP-848: regex pattern matching subscribed topics. Null means
    /// "unchanged since last heartbeat" — the client either uses
    /// <see cref="SubscribedTopicNames"/> or this regex, never both.
    /// </summary>
    public string? SubscribedTopicRegex { get; init; }

    public sealed class Assignor
    {
        /// <summary>The topic ID.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The partition indexes.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);
        writer.WriteCompactString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteCompactString(InstanceId);
        writer.WriteCompactString(RackId);
        writer.WriteInt32(RebalanceTimeoutMs);

        if (SubscribedTopicNames == null)
        {
            writer.WriteVarInt(-1); // Null compact array
        }
        else
        {
            writer.WriteVarInt(SubscribedTopicNames.Count + 1);
            foreach (var topic in SubscribedTopicNames)
            {
                writer.WriteCompactString(topic);
            }
        }

        writer.WriteCompactString(ServerAssignor);

        if (ApiVersion >= 1)
        {
            writer.WriteCompactString(SubscribedTopicRegex);
        }

        if (TopicPartitions == null)
        {
            writer.WriteVarInt(-1); // Null compact array
        }
        else
        {
            writer.WriteVarInt(TopicPartitions.Count + 1);
            foreach (var assignor in TopicPartitions)
            {
                writer.WriteUuid(assignor.TopicId);
                writer.WriteVarInt(assignor.Partitions.Count + 1);
                foreach (var partition in assignor.Partitions)
                {
                    writer.WriteInt32(partition);
                }
                writer.WriteVarInt(0); // Assignor tagged fields
            }
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ConsumerGroupHeartbeatRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";
        var memberId = reader.ReadCompactString() ?? "";
        var memberEpoch = reader.ReadInt32();
        var instanceId = reader.ReadCompactString();
        var rackId = reader.ReadCompactString();
        var rebalanceTimeoutMs = reader.ReadInt32();

        List<string>? subscribedTopicNames = null;
        var topicCount = reader.ReadVarInt() - 1;
        if (topicCount >= 0)
        {
            subscribedTopicNames = new List<string>(topicCount);
            for (int i = 0; i < topicCount; i++)
            {
                subscribedTopicNames.Add(reader.ReadCompactString() ?? "");
            }
        }

        var serverAssignor = reader.ReadCompactString();

        // v1+ adds SubscribedTopicRegex between ServerAssignor and TopicPartitions
        // (KIP-848 Phase II). Older clients send v0; reading the field at v0 would
        // misalign the rest of the body, so gate on apiVersion.
        string? subscribedTopicRegex = null;
        if (apiVersion >= 1)
        {
            subscribedTopicRegex = reader.ReadCompactString();
        }

        List<Assignor>? topicPartitions = null;
        var assignorCount = reader.ReadVarInt() - 1;
        if (assignorCount >= 0)
        {
            topicPartitions = new List<Assignor>(assignorCount);
            for (int i = 0; i < assignorCount; i++)
            {
                var topicId = reader.ReadUuid();
                var partCount = reader.ReadVarInt() - 1;
                var partitions = new List<int>(partCount);
                for (int j = 0; j < partCount; j++)
                {
                    partitions.Add(reader.ReadInt32());
                }
                reader.SkipTaggedFields();

                topicPartitions.Add(new Assignor
                {
                    TopicId = topicId,
                    Partitions = partitions
                });
            }
        }

        reader.SkipTaggedFields();

        return new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            InstanceId = instanceId,
            RackId = rackId,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
            SubscribedTopicNames = subscribedTopicNames,
            ServerAssignor = serverAssignor,
            SubscribedTopicRegex = subscribedTopicRegex,
            TopicPartitions = topicPartitions
        };
    }
}

/// <summary>
/// Kafka ConsumerGroupHeartbeat response (API Key 68, v0-0).
/// </summary>
public sealed class ConsumerGroupHeartbeatResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The top-level error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>The member ID.</summary>
    public string? MemberId { get; init; }

    /// <summary>The member epoch.</summary>
    public int MemberEpoch { get; init; }

    /// <summary>The heartbeat interval in milliseconds.</summary>
    public int HeartbeatIntervalMs { get; init; }

    /// <summary>The assignment.</summary>
    public Assignment? MemberAssignment { get; init; }

    public sealed class Assignment
    {
        /// <summary>The assigned topics/partitions.</summary>
        public required List<TopicPartitions> TopicPartitions { get; init; }
    }

    public sealed class TopicPartitions
    {
        /// <summary>The topic ID.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The partition indexes.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);
        writer.WriteCompactString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteInt32(HeartbeatIntervalMs);

        // Apache Kafka flexible-protocol nullable-struct encoding (per KIP-848 spec
        // and the generated MessageDataGenerator code): one signed byte presence
        // marker, NOT a compact array.
        //   null     → -1  (0xFF)
        //   non-null →  1, then the struct's fields plus its trailing tagged-fields varint
        // Encoding it as `varint(N+1)` (compactArray) makes librdkafka decode the
        // marker as "0 elements" and silently discard the assignment.
        if (MemberAssignment == null)
        {
            writer.WriteInt8(-1);
        }
        else
        {
            writer.WriteInt8(1);
            writer.WriteVarInt(MemberAssignment.TopicPartitions.Count + 1);
            foreach (var tp in MemberAssignment.TopicPartitions)
            {
                writer.WriteUuid(tp.TopicId);
                writer.WriteVarInt(tp.Partitions.Count + 1);
                foreach (var partition in tp.Partitions)
                {
                    writer.WriteInt32(partition);
                }
                writer.WriteVarInt(0); // TopicPartitions tagged fields
            }
            writer.WriteVarInt(0); // Assignment struct tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ConsumerGroupHeartbeatResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();
        var memberId = reader.ReadCompactString();
        var memberEpoch = reader.ReadInt32();
        var heartbeatIntervalMs = reader.ReadInt32();

        // Nullable-struct presence marker: signed byte, < 0 means null.
        Assignment? memberAssignment = null;
        var presence = reader.ReadInt8();
        if (presence >= 0)
        {
            var tpCount = reader.ReadVarInt() - 1;
            var topicPartitions = new List<TopicPartitions>(Math.Max(tpCount, 0));
            for (int i = 0; i < tpCount; i++)
            {
                var topicId = reader.ReadUuid();
                var partCount = reader.ReadVarInt() - 1;
                var partitions = new List<int>(partCount);
                for (int j = 0; j < partCount; j++)
                {
                    partitions.Add(reader.ReadInt32());
                }
                reader.SkipTaggedFields();

                topicPartitions.Add(new TopicPartitions
                {
                    TopicId = topicId,
                    Partitions = partitions
                });
            }
            reader.SkipTaggedFields();
            memberAssignment = new Assignment { TopicPartitions = topicPartitions };
        }

        reader.SkipTaggedFields();

        return new ConsumerGroupHeartbeatResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            HeartbeatIntervalMs = heartbeatIntervalMs,
            MemberAssignment = memberAssignment
        };
    }
}

/// <summary>
/// Kafka ConsumerGroupDescribe request (API Key 69, v0-0).
/// KIP-848: Describe consumer groups using the new protocol.
/// </summary>
public sealed class ConsumerGroupDescribeRequest : KafkaRequest
{
    /// <summary>The IDs of the groups to describe.</summary>
    public required List<string> GroupIds { get; init; }

    /// <summary>Whether to include authorized operations.</summary>
    public bool IncludeAuthorizedOperations { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteVarInt(GroupIds.Count + 1);
        foreach (var groupId in GroupIds)
        {
            writer.WriteCompactString(groupId);
        }

        writer.WriteBoolean(IncludeAuthorizedOperations);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ConsumerGroupDescribeRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupCount = reader.ReadVarInt() - 1;
        var groupIds = new List<string>(groupCount);
        for (int i = 0; i < groupCount; i++)
        {
            groupIds.Add(reader.ReadCompactString() ?? "");
        }

        var includeAuthorizedOperations = reader.ReadBoolean();

        reader.SkipTaggedFields();

        return new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupIds = groupIds,
            IncludeAuthorizedOperations = includeAuthorizedOperations
        };
    }
}

/// <summary>
/// Kafka ConsumerGroupDescribe response (API Key 69, v0-0).
/// </summary>
public sealed class ConsumerGroupDescribeResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>Each described group.</summary>
    public required List<DescribedGroup> Groups { get; init; }

    public sealed class DescribedGroup
    {
        /// <summary>The describe error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The describe error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The group ID string.</summary>
        public required string GroupId { get; init; }

        /// <summary>The group state string.</summary>
        public required string GroupState { get; init; }

        /// <summary>The group epoch.</summary>
        public int GroupEpoch { get; init; }

        /// <summary>The assignment epoch.</summary>
        public int AssignmentEpoch { get; init; }

        /// <summary>The selected assignor.</summary>
        public string? AssignorName { get; init; }

        /// <summary>The group members.</summary>
        public required List<Member> Members { get; init; }

        /// <summary>32-bit bitfield representing authorized operations.</summary>
        public int AuthorizedOperations { get; init; } = int.MinValue;
    }

    public sealed class Member
    {
        /// <summary>The member ID.</summary>
        public required string MemberId { get; init; }

        /// <summary>The static member instance ID.</summary>
        public string? InstanceId { get; init; }

        /// <summary>The rack ID.</summary>
        public string? RackId { get; init; }

        /// <summary>The member epoch.</summary>
        public int MemberEpoch { get; init; }

        /// <summary>The client ID.</summary>
        public required string ClientId { get; init; }

        /// <summary>The client host.</summary>
        public required string ClientHost { get; init; }

        /// <summary>The subscribed topics.</summary>
        public required List<string> SubscribedTopicNames { get; init; }

        /// <summary>The subscribed topic regex.</summary>
        public string? SubscribedTopicRegex { get; init; }

        /// <summary>The assignment.</summary>
        public required Assignment MemberAssignment { get; init; }

        /// <summary>The target assignment.</summary>
        public required Assignment TargetAssignment { get; init; }
    }

    public sealed class Assignment
    {
        /// <summary>The assigned topics/partitions.</summary>
        public required List<TopicPartitions> TopicPartitions { get; init; }
    }

    public sealed class TopicPartitions
    {
        /// <summary>The topic ID.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The partition indexes.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);

        writer.WriteVarInt(Groups.Count + 1);
        foreach (var group in Groups)
        {
            writer.WriteInt16((short)group.ErrorCode);
            writer.WriteCompactString(group.ErrorMessage);
            writer.WriteCompactString(group.GroupId);
            writer.WriteCompactString(group.GroupState);
            writer.WriteInt32(group.GroupEpoch);
            writer.WriteInt32(group.AssignmentEpoch);
            writer.WriteCompactString(group.AssignorName);

            writer.WriteVarInt(group.Members.Count + 1);
            foreach (var member in group.Members)
            {
                writer.WriteCompactString(member.MemberId);
                writer.WriteCompactString(member.InstanceId);
                writer.WriteCompactString(member.RackId);
                writer.WriteInt32(member.MemberEpoch);
                writer.WriteCompactString(member.ClientId);
                writer.WriteCompactString(member.ClientHost);

                writer.WriteVarInt(member.SubscribedTopicNames.Count + 1);
                foreach (var topic in member.SubscribedTopicNames)
                {
                    writer.WriteCompactString(topic);
                }

                writer.WriteCompactString(member.SubscribedTopicRegex);

                // Member assignment
                WriteAssignment(writer, member.MemberAssignment);

                // Target assignment
                WriteAssignment(writer, member.TargetAssignment);

                writer.WriteVarInt(0); // Member tagged fields
            }

            writer.WriteInt32(group.AuthorizedOperations);

            writer.WriteVarInt(0); // Group tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    private static void WriteAssignment(KafkaProtocolWriter writer, Assignment assignment)
    {
        writer.WriteVarInt(assignment.TopicPartitions.Count + 1);
        foreach (var tp in assignment.TopicPartitions)
        {
            writer.WriteUuid(tp.TopicId);
            writer.WriteVarInt(tp.Partitions.Count + 1);
            foreach (var partition in tp.Partitions)
            {
                writer.WriteInt32(partition);
            }
            writer.WriteVarInt(0); // TopicPartitions tagged fields
        }
        writer.WriteVarInt(0); // Assignment tagged fields
    }

    public static ConsumerGroupDescribeResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();

        var groupCount = reader.ReadVarInt() - 1;
        var groups = new List<DescribedGroup>(groupCount);

        for (int i = 0; i < groupCount; i++)
        {
            var errorCode = (ErrorCode)reader.ReadInt16();
            var errorMessage = reader.ReadCompactString();
            var groupId = reader.ReadCompactString() ?? "";
            var groupState = reader.ReadCompactString() ?? "";
            var groupEpoch = reader.ReadInt32();
            var assignmentEpoch = reader.ReadInt32();
            var assignorName = reader.ReadCompactString();

            var memberCount = reader.ReadVarInt() - 1;
            var members = new List<Member>(memberCount);

            for (int j = 0; j < memberCount; j++)
            {
                var memberId = reader.ReadCompactString() ?? "";
                var instanceId = reader.ReadCompactString();
                var rackId = reader.ReadCompactString();
                var memberEpoch = reader.ReadInt32();
                var clientIdMember = reader.ReadCompactString() ?? "";
                var clientHost = reader.ReadCompactString() ?? "";

                var topicCount = reader.ReadVarInt() - 1;
                var subscribedTopicNames = new List<string>(topicCount);
                for (int k = 0; k < topicCount; k++)
                {
                    subscribedTopicNames.Add(reader.ReadCompactString() ?? "");
                }

                var subscribedTopicRegex = reader.ReadCompactString();

                var memberAssignment = ReadAssignment(reader);
                var targetAssignment = ReadAssignment(reader);

                reader.SkipTaggedFields();

                members.Add(new Member
                {
                    MemberId = memberId,
                    InstanceId = instanceId,
                    RackId = rackId,
                    MemberEpoch = memberEpoch,
                    ClientId = clientIdMember,
                    ClientHost = clientHost,
                    SubscribedTopicNames = subscribedTopicNames,
                    SubscribedTopicRegex = subscribedTopicRegex,
                    MemberAssignment = memberAssignment,
                    TargetAssignment = targetAssignment
                });
            }

            var authorizedOperations = reader.ReadInt32();

            reader.SkipTaggedFields();

            groups.Add(new DescribedGroup
            {
                ErrorCode = errorCode,
                ErrorMessage = errorMessage,
                GroupId = groupId,
                GroupState = groupState,
                GroupEpoch = groupEpoch,
                AssignmentEpoch = assignmentEpoch,
                AssignorName = assignorName,
                Members = members,
                AuthorizedOperations = authorizedOperations
            });
        }

        reader.SkipTaggedFields();

        return new ConsumerGroupDescribeResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Groups = groups
        };
    }

    private static Assignment ReadAssignment(KafkaProtocolReader reader)
    {
        var tpCount = reader.ReadVarInt() - 1;
        var topicPartitions = new List<TopicPartitions>(tpCount);

        for (int i = 0; i < tpCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<int>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                partitions.Add(reader.ReadInt32());
            }
            reader.SkipTaggedFields();

            topicPartitions.Add(new TopicPartitions
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new Assignment { TopicPartitions = topicPartitions };
    }
}
