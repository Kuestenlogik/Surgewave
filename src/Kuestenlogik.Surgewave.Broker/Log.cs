using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Source-generated high-performance logging for SurgewaveBroker
/// </summary>
internal static partial class Log
{
    // Broker lifecycle
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting Surgewave broker on {Host}:{Port}")]
    public static partial void BrokerStarting(ILogger logger, string host, int port);

    [LoggerMessage(Level = LogLevel.Information, Message = "Data directory: {DataDirectory}")]
    public static partial void DataDirectory(ILogger logger, string dataDirectory);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker started successfully")]
    public static partial void BrokerStarted(ILogger logger);

    // Client connections
    [LoggerMessage(Level = LogLevel.Information, Message = "Client connected from {Endpoint}")]
    public static partial void ClientConnected(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Client disconnected: {Endpoint}")]
    public static partial void ClientDisconnected(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error accepting client")]
    public static partial void ErrorAcceptingClient(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Client error for {Endpoint}")]
    public static partial void ClientError(ILogger logger, Exception ex, object? endpoint);

    // Request handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Waiting for request")]
    public static partial void WaitingForRequest(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Received {ApiKey} request (size: {Size}, correlationId: {CorrelationId})")]
    public static partial void RequestReceived(ILogger logger, object? endpoint, object apiKey, int size, int correlationId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Sending response for correlationId: {CorrelationId}")]
    public static partial void SendingResponse(ILogger logger, object? endpoint, int correlationId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Response sent successfully")]
    public static partial void ResponseSent(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "[{Endpoint}] Received non-Kafka request type: {RequestType}")]
    public static partial void NonKafkaRequest(ILogger logger, object? endpoint, string requestType);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] End of stream")]
    public static partial void EndOfStream(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] IO error")]
    public static partial void IoError(ILogger logger, Exception ex, object? endpoint);

    // Produce handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored RecordBatch for {Topic}-{Partition}, baseOffset={BaseOffset}, size={Size} bytes")]
    public static partial void RecordBatchStored(ILogger logger, string topic, int partition, long baseOffset, int size);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error producing to {Topic}-{Partition}")]
    public static partial void ProduceError(ILogger logger, Exception ex, string topic, int partition);

    // Fetch handling
    [LoggerMessage(Level = LogLevel.Trace, Message = "Read {BatchCount} batches from {Topic}-{Partition} at offset {FetchOffset}")]
    public static partial void BatchesRead(ILogger logger, int batchCount, string topic, int partition, long fetchOffset);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Combined {BatchCount} batches into {RecordSetSize} bytes")]
    public static partial void BatchesCombined(ILogger logger, int batchCount, int recordSetSize);

    [LoggerMessage(Level = LogLevel.Trace, Message = "LogStartOffset={LogStartOffset}, HighWatermark={HighWatermark}")]
    public static partial void LogOffsets(ILogger logger, long logStartOffset, long highWatermark);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error fetching from {Topic}-{Partition}")]
    public static partial void FetchError(ILogger logger, Exception ex, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Trace, Message = "[FetchDebug] {Topic}-{Partition} fetchOffset={FetchOffset}, logStartOffset={LogStartOffset}, nextOffset={NextOffset}, logExists={LogExists}")]
    public static partial void FetchDebug(ILogger logger, string topic, int partition, long fetchOffset, long logStartOffset, long nextOffset, bool logExists);

    // Metadata handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for ALL topics, found {TopicCount} topics")]
    public static partial void MetadataAllTopics(ILogger logger, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for empty topic list (treating as ALL), found {TopicCount} topics")]
    public static partial void MetadataEmptyList(ILogger logger, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for specific topics: {Topics}")]
    public static partial void MetadataSpecificTopics(ILogger logger, string topics);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Processing metadata for topic: {Topic}")]
    public static partial void ProcessingMetadata(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Auto-creating topic: {Topic}")]
    public static partial void AutoCreatingTopic(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Topic created: {Topic}, partitions: {PartitionCount}")]
    public static partial void TopicCreated(ILogger logger, string topic, int partitionCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Topic deleted: {Topic}")]
    public static partial void TopicDeleted(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error creating topic: {Topic}")]
    public static partial void CreateTopicError(ILogger logger, Exception ex, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error deleting topic: {Topic}")]
    public static partial void DeleteTopicError(ILogger logger, Exception ex, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata response: Broker[0] NodeId={NodeId}, Host={Host}, Port={Port}")]
    public static partial void MetadataResponse(ILogger logger, int nodeId, string host, int port);

    [LoggerMessage(Level = LogLevel.Debug, Message = "  Topic: {TopicName}, ErrorCode={ErrorCode}, Partitions={PartitionCount}")]
    public static partial void MetadataTopicInfo(ILogger logger, string topicName, object errorCode, int partitionCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "    Partition {PartitionId}: Leader={Leader}, Replicas=[{Replicas}], ISR=[{Isr}], ErrorCode={ErrorCode}")]
    public static partial void MetadataPartitionInfo(ILogger logger, int partitionId, int leader, string replicas, string isr, object errorCode);

    // ConsumerGroupCoordinator - SyncGroup handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] ApiVersion={ApiVersion}, GroupId={GroupId}, MemberId={MemberId}, GenerationId={GenerationId}")]
    public static partial void SyncGroupRequest(ILogger logger, short apiVersion, string groupId, string memberId, int generationId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] Assignments.Length={AssignmentsLength}")]
    public static partial void SyncGroupAssignmentsCount(ILogger logger, int assignmentsLength);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] Group not found: {GroupId}")]
    public static partial void SyncGroupNotFound(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] Leader sending assignments for {MemberCount} members")]
    public static partial void SyncGroupLeaderAssignments(ILogger logger, int memberCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] Assignment for {MemberId}: {AssignmentLength} bytes")]
    public static partial void SyncGroupMemberAssignment(ILogger logger, string memberId, int assignmentLength);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[SyncGroup] Returning assignment for {MemberId}: {AssignmentLength} bytes")]
    public static partial void SyncGroupReturningAssignment(ILogger logger, string memberId, int assignmentLength);

    // RecordBatchSerializer - Trace logging
    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordBatch.Length={Length}")]
    public static partial void ParseRecordBatchLength(ILogger logger, int length);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: baseOffset={BaseOffset}, batchLength={BatchLength}, magic={Magic}")]
    public static partial void ParseRecordBatchHeader(ILogger logger, long baseOffset, int batchLength, byte magic);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordCount={RecordCount}, stream.Position={StreamPosition}")]
    public static partial void ParseRecordBatchRecordCount(ILogger logger, int recordCount, long streamPosition);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordsSize={RecordsSize}, remaining bytes={RemainingBytes}")]
    public static partial void ParseRecordBatchRecordsSize(ILogger logger, int recordsSize, int remainingBytes);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: actually read {BytesRead} bytes")]
    public static partial void ParseRecordBatchBytesRead(ILogger logger, int bytesRead);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ParseRecordBatch: Decompressed {CompressionType}: {CompressedSize} -> {DecompressedSize} bytes")]
    public static partial void ParseRecordBatchDecompressed(ILogger logger, string compressionType, int compressedSize, int decompressedSize);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Parsing record {RecordIndex}, position={Position}, remaining={Remaining}")]
    public static partial void ParseRecordBatchParsingRecord(ILogger logger, int recordIndex, int position, int remaining);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} length={RecordLength} (raw={RawLength})")]
    public static partial void ParseRecordBatchRecordLength(ILogger logger, int recordIndex, int recordLength, int rawLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} attributes={Attributes}")]
    public static partial void ParseRecordBatchAttributes(ILogger logger, int recordIndex, sbyte attributes);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} timestampDelta={TimestampDelta} (raw={RawTimestampDelta})")]
    public static partial void ParseRecordBatchTimestampDelta(ILogger logger, int recordIndex, long timestampDelta, long rawTimestampDelta);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} offsetDelta={OffsetDelta} (raw={RawOffsetDelta})")]
    public static partial void ParseRecordBatchOffsetDelta(ILogger logger, int recordIndex, int offsetDelta, int rawOffsetDelta);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} keyLength={KeyLength} (raw={RawKeyLength})")]
    public static partial void ParseRecordBatchKeyLength(ILogger logger, int recordIndex, int keyLength, int rawKeyLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} valueLength={ValueLength} (raw={RawValueLength})")]
    public static partial void ParseRecordBatchValueLength(ILogger logger, int recordIndex, int valueLength, int rawValueLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Message created: Offset={Offset}, Timestamp={Timestamp}, KeyLength={KeyLength}, ValueLength={ValueLength}")]
    public static partial void ParseRecordBatchMessageCreated(ILogger logger, long offset, long timestamp, int keyLength, int valueLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Successfully parsed {MessageCount} messages")]
    public static partial void ParseRecordBatchComplete(ILogger logger, int messageCount);

    // Shutdown handling
    [LoggerMessage(Level = LogLevel.Information, Message = "Broker shutdown starting...")]
    public static partial void ShutdownStarting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopped accepting new connections")]
    public static partial void ShutdownStoppedListener(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for {ClientCount} active client(s) to complete")]
    public static partial void ShutdownWaitingForClients(ILogger logger, int clientCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Shutdown timeout: {RemainingCount} client(s) still active, forcing disconnect")]
    public static partial void ShutdownClientTimeout(ILogger logger, int remainingCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error during client task completion")]
    public static partial void ShutdownClientError(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Disposing resources")]
    public static partial void ShutdownDisposingResources(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker shutdown complete")]
    public static partial void ShutdownComplete(ILogger logger);

    // OffsetStore logging
    [LoggerMessage(Level = LogLevel.Debug, Message = "OffsetStore: Loaded group {GroupId} with {OffsetCount} committed offsets")]
    public static partial void OffsetStoreLoadedGroup(ILogger logger, string groupId, int offsetCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "OffsetStore: Failed to load offsets from {FilePath}")]
    public static partial void OffsetStoreLoadError(ILogger logger, string filePath, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "OffsetStore: Initialized with {GroupCount} consumer groups")]
    public static partial void OffsetStoreInitialized(ILogger logger, int groupCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "OffsetStore: Failed to persist offsets for group {GroupId}")]
    public static partial void OffsetStorePersistError(ILogger logger, string groupId, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "OffsetStore: Committed offset {Offset} for {GroupId}/{Topic}/{Partition}")]
    public static partial void OffsetStoreCommitted(ILogger logger, long offset, string groupId, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "OffsetStore: Flushed {GroupCount} dirty groups to disk")]
    public static partial void OffsetStoreFlushed(ILogger logger, int groupCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "OffsetStore: Deleted group {GroupId}")]
    public static partial void OffsetStoreGroupDeleted(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Error, Message = "OffsetStore: Failed to delete group {GroupId}")]
    public static partial void OffsetStoreDeleteError(ILogger logger, string groupId, Exception ex);

    // OffsetFetch debugging
    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] GroupId={GroupId}, Topics={TopicCount}")]
    public static partial void OffsetFetchRequest(ILogger logger, string groupId, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] Group {GroupId} not in memory, checking store")]
    public static partial void OffsetFetchGroupNotInMemory(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] Group {GroupId} in memory, CommittedOffsets count={Count}")]
    public static partial void OffsetFetchGroupInMemory(ILogger logger, string groupId, int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] From store: {GroupId}/{Topic}/{Partition} -> {Offset}")]
    public static partial void OffsetFetchFromStore(ILogger logger, string groupId, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] From memory: key={Key} -> {Offset}")]
    public static partial void OffsetFetchFromMemory(ILogger logger, string key, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[OffsetFetch] Fallback to store: {GroupId}/{Topic}/{Partition} -> {Offset}")]
    public static partial void OffsetFetchFallbackToStore(ILogger logger, string groupId, string topic, int partition, long offset);

    // JoinGroup debugging
    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] ApiVersion={ApiVersion}, GroupId={GroupId}, MemberId={MemberId}, Protocols={ProtocolCount}, MetadataSize={MetadataSize}")]
    public static partial void JoinGroupRequest(ILogger logger, short apiVersion, string groupId, string memberId, int protocolCount, int metadataSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Created new group: {GroupId}")]
    public static partial void JoinGroupCreated(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Added member {MemberId}, metadata size={MetadataSize}")]
    public static partial void JoinGroupMemberAdded(ILogger logger, string memberId, int metadataSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Updated member {MemberId}, metadata size={MetadataSize}")]
    public static partial void JoinGroupMemberUpdated(ILogger logger, string memberId, int metadataSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Member={MemberId}, Leader={LeaderId}, IsLeader={IsLeader}, TotalMembers={TotalMembers}")]
    public static partial void JoinGroupLeaderInfo(ILogger logger, string memberId, string leaderId, bool isLeader, int totalMembers);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Response for {MemberId}: returning {MemberCount} members, total metadata bytes={TotalMetadataBytes}")]
    public static partial void JoinGroupResponse(ILogger logger, string memberId, int memberCount, int totalMetadataBytes);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[JoinGroup] Removed stale member {MemberId} (no heartbeat within {TimeoutMs}ms)")]
    public static partial void JoinGroupRemovedStaleMember(ILogger logger, string memberId, double timeoutMs);

    // TLS handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] TLS handshake starting")]
    public static partial void TlsHandshakeStarting(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] TLS handshake completed")]
    public static partial void TlsHandshakeCompleted(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "[{Endpoint}] TLS handshake failed")]
    public static partial void TlsHandshakeFailed(ILogger logger, Exception ex, object? endpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "TLS enabled: {TlsSummary}")]
    public static partial void TlsConfigured(ILogger logger, string tlsSummary);

    // DeleteGroups handling
    [LoggerMessage(Level = LogLevel.Debug, Message = "[DeleteGroups] Request for {GroupCount} groups")]
    public static partial void DeleteGroupsRequest(ILogger logger, int groupCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "[DeleteGroups] Deleted group {GroupId}")]
    public static partial void DeleteGroupsDeleted(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[DeleteGroups] Group {GroupId} not empty (has {MemberCount} members)")]
    public static partial void DeleteGroupsNotEmpty(ILogger logger, string groupId, int memberCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[DeleteGroups] Group {GroupId} not found")]
    public static partial void DeleteGroupsNotFound(ILogger logger, string groupId);
}
