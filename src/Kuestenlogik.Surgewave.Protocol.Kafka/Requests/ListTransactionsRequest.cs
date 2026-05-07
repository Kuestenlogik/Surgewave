namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ListTransactions request (API Key 66, v0-2).
/// Lists all currently active transactions on the cluster.
/// Used for monitoring transaction state and identifying stuck transactions.
/// </summary>
/// <remarks>
/// Version history:
/// - v0: StateFilters + ProducerIdFilters
/// - v1: + DurationFilter (KIP-994)
/// - v2: + TransactionalIdPattern regex (KIP-1152)
/// </remarks>
public sealed class ListTransactionsRequest : KafkaRequest
{
    /// <summary>
    /// Filter transactions by current state.
    /// If empty, list all transactions regardless of state.
    /// Valid states: Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit, CompleteAbort, Dead, PrepareEpochFence
    /// </summary>
    public List<string> StateFilters { get; init; } = [];

    /// <summary>
    /// Filter transactions by producer ID.
    /// If empty, list all transactions regardless of producer.
    /// </summary>
    public List<long> ProducerIdFilters { get; init; } = [];

    /// <summary>
    /// Filter transactions by duration in milliseconds (v1+, KIP-994).
    /// Only show transactions that have been running longer than this duration.
    /// -1 means no filter.
    /// </summary>
    public long DurationFilter { get; init; } = -1;

    /// <summary>
    /// Regular-expression pattern matched against the transactional id (v2+, KIP-1152).
    /// Null or empty means no filter; non-empty restricts the listing to ids that
    /// match the supplied .NET regex syntax.
    /// </summary>
    public string? TransactionalIdPattern { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible (v0+)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        // StateFilters array (compact)
        writer.WriteVarInt(StateFilters.Count + 1);
        foreach (var state in StateFilters)
        {
            writer.WriteCompactString(state);
        }

        // ProducerIdFilters array (compact)
        writer.WriteVarInt(ProducerIdFilters.Count + 1);
        foreach (var producerId in ProducerIdFilters)
        {
            writer.WriteInt64(producerId);
        }

        // v1+: DurationFilter
        if (ApiVersion >= 1)
        {
            writer.WriteInt64(DurationFilter);
        }

        // v2+: TransactionalIdPattern (KIP-1152)
        if (ApiVersion >= 2)
        {
            writer.WriteCompactString(TransactionalIdPattern);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListTransactionsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stateCount = reader.ReadVarInt() - 1;
        var stateFilters = new List<string>(stateCount);
        for (int i = 0; i < stateCount; i++)
        {
            stateFilters.Add(reader.ReadCompactString() ?? "");
        }

        var producerIdCount = reader.ReadVarInt() - 1;
        var producerIdFilters = new List<long>(producerIdCount);
        for (int i = 0; i < producerIdCount; i++)
        {
            producerIdFilters.Add(reader.ReadInt64());
        }

        var durationFilter = apiVersion >= 1 ? reader.ReadInt64() : -1L;
        var transactionalIdPattern = apiVersion >= 2 ? reader.ReadCompactString() : null;

        reader.SkipTaggedFields();

        return new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            StateFilters = stateFilters,
            ProducerIdFilters = producerIdFilters,
            DurationFilter = durationFilter,
            TransactionalIdPattern = transactionalIdPattern,
        };
    }
}

/// <summary>
/// Kafka ListTransactions response (API Key 66, v0-1).
/// </summary>
public sealed class ListTransactionsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// List of brokers that could not be reached.
    /// The list of transactions may be incomplete if any brokers were down.
    /// </summary>
    public List<int> UnknownStateFilters { get; init; } = [];

    /// <summary>The list of transactions.</summary>
    public required List<TransactionListing> TransactionStates { get; init; }

    public sealed class TransactionListing
    {
        /// <summary>The transactional ID.</summary>
        public required string TransactionalId { get; init; }

        /// <summary>The producer ID.</summary>
        public required long ProducerId { get; init; }

        /// <summary>
        /// The current transaction state:
        /// Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit, CompleteAbort, Dead, PrepareEpochFence
        /// </summary>
        public required string TransactionState { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        // UnknownStateFilters array (compact)
        writer.WriteVarInt(UnknownStateFilters.Count + 1);
        foreach (var filter in UnknownStateFilters)
        {
            writer.WriteInt32(filter);
        }

        // TransactionStates array (compact)
        writer.WriteVarInt(TransactionStates.Count + 1);
        foreach (var txn in TransactionStates)
        {
            writer.WriteCompactString(txn.TransactionalId);
            writer.WriteInt64(txn.ProducerId);
            writer.WriteCompactString(txn.TransactionState);
            writer.WriteVarInt(0); // Transaction tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListTransactionsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        var unknownStateFilterCount = reader.ReadVarInt() - 1;
        var unknownStateFilters = new List<int>(unknownStateFilterCount);
        for (int i = 0; i < unknownStateFilterCount; i++)
        {
            unknownStateFilters.Add(reader.ReadInt32());
        }

        var txnCount = reader.ReadVarInt() - 1;
        var transactions = new List<TransactionListing>(txnCount);

        for (int i = 0; i < txnCount; i++)
        {
            var transactionalId = reader.ReadCompactString() ?? "";
            var producerId = reader.ReadInt64();
            var transactionState = reader.ReadCompactString() ?? "";
            reader.SkipTaggedFields();

            transactions.Add(new TransactionListing
            {
                TransactionalId = transactionalId,
                ProducerId = producerId,
                TransactionState = transactionState
            });
        }

        reader.SkipTaggedFields();

        return new ListTransactionsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            UnknownStateFilters = unknownStateFilters,
            TransactionStates = transactions
        };
    }
}
