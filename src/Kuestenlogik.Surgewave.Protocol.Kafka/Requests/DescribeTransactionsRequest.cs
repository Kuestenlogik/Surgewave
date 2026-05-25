namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeTransactions request (API Key 65, v0-0).
/// Describes the state of transactions by their transactional IDs.
/// Used for monitoring and debugging transaction state.
/// </summary>
public sealed class DescribeTransactionsRequest : KafkaRequest
{
    /// <summary>The transactional IDs to describe.</summary>
    public required List<string> TransactionalIds { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible (v0+)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        // TransactionalIds array (compact)
        writer.WriteVarInt(TransactionalIds.Count + 1);
        foreach (var txnId in TransactionalIds)
        {
            writer.WriteCompactString(txnId);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeTransactionsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var count = reader.ReadVarInt() - 1;
        var transactionalIds = new List<string>(count);

        for (int i = 0; i < count; i++)
        {
            transactionalIds.Add(reader.ReadCompactString() ?? "");
        }

        reader.SkipTaggedFields();

        return new DescribeTransactionsRequest
        {
            ApiKey = ApiKey.DescribeTransactions,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalIds = transactionalIds
        };
    }
}

/// <summary>
/// Kafka DescribeTransactions response (API Key 65, v0-0).
/// </summary>
public sealed class DescribeTransactionsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The transaction states.</summary>
    public required List<TransactionState> TransactionStates { get; init; }

    public sealed class TransactionState
    {
        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The transactional ID.</summary>
        public required string TransactionalId { get; init; }

        /// <summary>
        /// The transaction state:
        /// Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit, CompleteAbort, Dead, PrepareEpochFence
        /// </summary>
        public required string State { get; init; }

        /// <summary>The timeout in milliseconds.</summary>
        public int TransactionTimeoutMs { get; init; }

        /// <summary>The time the transaction started, in milliseconds since epoch.</summary>
        public long TransactionStartTimeMs { get; init; } = -1;

        /// <summary>The producer ID associated with the transaction.</summary>
        public long ProducerId { get; init; } = -1;

        /// <summary>The producer epoch associated with the transaction.</summary>
        public short ProducerEpoch { get; init; } = -1;

        /// <summary>The set of partitions included in the current transaction (if active).</summary>
        public required List<TopicPartition> Topics { get; init; }
    }

    public sealed class TopicPartition
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The partition indices.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);

        // TransactionStates array (compact)
        writer.WriteVarInt(TransactionStates.Count + 1);
        foreach (var txn in TransactionStates)
        {
            writer.WriteInt16((short)txn.ErrorCode);
            writer.WriteCompactString(txn.TransactionalId);
            writer.WriteCompactString(txn.State);
            writer.WriteInt32(txn.TransactionTimeoutMs);
            writer.WriteInt64(txn.TransactionStartTimeMs);
            writer.WriteInt64(txn.ProducerId);
            writer.WriteInt16(txn.ProducerEpoch);

            // Topics array (compact)
            writer.WriteVarInt(txn.Topics.Count + 1);
            foreach (var topic in txn.Topics)
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition);
                }
                writer.WriteVarInt(0); // Topic tagged fields
            }

            writer.WriteVarInt(0); // Transaction tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeTransactionsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();

        var txnCount = reader.ReadVarInt() - 1;
        var transactions = new List<TransactionState>(txnCount);

        for (int i = 0; i < txnCount; i++)
        {
            var errorCode = (ErrorCode)reader.ReadInt16();
            var transactionalId = reader.ReadCompactString() ?? "";
            var state = reader.ReadCompactString() ?? "";
            var transactionTimeoutMs = reader.ReadInt32();
            var transactionStartTimeMs = reader.ReadInt64();
            var producerId = reader.ReadInt64();
            var producerEpoch = reader.ReadInt16();

            var topicCount = reader.ReadVarInt() - 1;
            var topics = new List<TopicPartition>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicName = reader.ReadCompactString() ?? "";
                var partitionCount = reader.ReadVarInt() - 1;
                var partitions = new List<int>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    partitions.Add(reader.ReadInt32());
                }

                reader.SkipTaggedFields();

                topics.Add(new TopicPartition
                {
                    Topic = topicName,
                    Partitions = partitions
                });
            }

            reader.SkipTaggedFields();

            transactions.Add(new TransactionState
            {
                ErrorCode = errorCode,
                TransactionalId = transactionalId,
                State = state,
                TransactionTimeoutMs = transactionTimeoutMs,
                TransactionStartTimeMs = transactionStartTimeMs,
                ProducerId = producerId,
                ProducerEpoch = producerEpoch,
                Topics = topics
            });
        }

        reader.SkipTaggedFields();

        return new DescribeTransactionsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            TransactionStates = transactions
        };
    }
}
