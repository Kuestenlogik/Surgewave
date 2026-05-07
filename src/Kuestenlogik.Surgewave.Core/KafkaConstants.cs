namespace Kuestenlogik.Surgewave.Core;

/// <summary>
/// Kafka protocol constants and format specifications
/// </summary>
public static class KafkaConstants
{
    // ===================================================================
    // RecordBatch Format (Kafka Magic v2) - Byte offsets and sizes
    // ===================================================================

    /// <summary>
    /// Field sizes for Kafka RecordBatch format (Magic v2)
    /// </summary>
    public static class RecordBatch
    {
        /// <summary>Base offset of the first record in the batch (8 bytes)</summary>
        public const int BaseOffsetSize = 8;

        /// <summary>Length in bytes of the partition leader epoch, magic byte, and record data (4 bytes)</summary>
        public const int LengthSize = 4;

        /// <summary>Partition leader epoch for fencing (4 bytes)</summary>
        public const int PartitionLeaderEpochSize = 4;

        /// <summary>Magic byte identifying format version (1 byte, always 2 for current format)</summary>
        public const int MagicSize = 1;

        /// <summary>CRC32-C checksum covering everything after this field (4 bytes)</summary>
        public const int CrcSize = 4;

        /// <summary>Batch-level flags including compression and transaction info (2 bytes)</summary>
        public const int AttributesSize = 2;

        /// <summary>Offset delta of last record in batch relative to base offset (4 bytes)</summary>
        public const int LastOffsetDeltaSize = 4;

        /// <summary>Timestamp of first record in batch (8 bytes, milliseconds since epoch)</summary>
        public const int BaseTimestampSize = 8;

        /// <summary>Timestamp of last record in batch (8 bytes, milliseconds since epoch)</summary>
        public const int MaxTimestampSize = 8;

        /// <summary>Producer ID for idempotent/transactional producers (8 bytes)</summary>
        public const int ProducerIdSize = 8;

        /// <summary>Producer epoch for fencing zombie producers (2 bytes)</summary>
        public const int ProducerEpochSize = 2;

        /// <summary>Base sequence number for idempotent producers (4 bytes)</summary>
        public const int BaseSequenceSize = 4;

        /// <summary>Number of records in this batch (4 bytes)</summary>
        public const int RecordsCountSize = 4;

        /// <summary>
        /// Total header size: 61 bytes
        /// Sum of all field sizes in the RecordBatch header
        /// </summary>
        public const int HeaderSize =
            BaseOffsetSize +
            LengthSize +
            PartitionLeaderEpochSize +
            MagicSize +
            CrcSize +
            AttributesSize +
            LastOffsetDeltaSize +
            BaseTimestampSize +
            MaxTimestampSize +
            ProducerIdSize +
            ProducerEpochSize +
            BaseSequenceSize +
            RecordsCountSize;

        /// <summary>
        /// Offset where CRC calculation starts (21 bytes from start)
        /// CRC covers everything after: magic + CRC field itself
        /// </summary>
        public const int CrcStartOffset =
            BaseOffsetSize +
            LengthSize +
            PartitionLeaderEpochSize +
            MagicSize +
            CrcSize;

        // Field offsets from start of RecordBatch
        public const int BaseOffsetOffset = 0;
        public const int LengthOffset = BaseOffsetOffset + BaseOffsetSize; // 8
        public const int PartitionLeaderEpochOffset = LengthOffset + LengthSize; // 12
        public const int MagicOffset = PartitionLeaderEpochOffset + PartitionLeaderEpochSize; // 16
        public const int CrcOffset = MagicOffset + MagicSize; // 17
        public const int AttributesOffset = CrcOffset + CrcSize; // 21
        public const int LastOffsetDeltaOffset = AttributesOffset + AttributesSize; // 23
        public const int BaseTimestampOffset = LastOffsetDeltaOffset + LastOffsetDeltaSize; // 27
        public const int MaxTimestampOffset = BaseTimestampOffset + BaseTimestampSize; // 35
        public const int ProducerIdOffset = MaxTimestampOffset + MaxTimestampSize; // 43
        public const int ProducerEpochOffset = ProducerIdOffset + ProducerIdSize; // 51
        public const int BaseSequenceOffset = ProducerEpochOffset + ProducerEpochSize; // 53
        public const int RecordsCountOffset = BaseSequenceOffset + BaseSequenceSize; // 57
    }

    // ===================================================================
    // VarInt/VarLong Encoding
    // ===================================================================

    /// <summary>
    /// Variable-length integer encoding sizes
    /// </summary>
    public static class VarInt
    {
        /// <summary>Maximum bytes for a varint (32-bit integer)</summary>
        public const int MaxSize = 5;

        /// <summary>Maximum bytes for a varlong (64-bit long)</summary>
        public const int VarLongMaxSize = 10;
    }

    // ===================================================================
    // RecordBatch Magic Versions
    // ===================================================================

    /// <summary>
    /// Kafka message format magic byte versions
    /// </summary>
    public static class Magic
    {
        /// <summary>Legacy message format (deprecated)</summary>
        public const byte V0 = 0;

        /// <summary>Message format with timestamps (deprecated)</summary>
        public const byte V1 = 1;

        /// <summary>Current RecordBatch format with compression, transactions, and idempotence</summary>
        public const byte V2 = 2;
    }

    // ===================================================================
    // Compression Types (bits 0-2 of attributes)
    // ===================================================================

    /// <summary>
    /// Compression codec types for RecordBatch
    /// </summary>
    public static class Compression
    {
        /// <summary>No compression</summary>
        public const int None = 0;

        /// <summary>GZIP compression</summary>
        public const int Gzip = 1;

        /// <summary>Snappy compression</summary>
        public const int Snappy = 2;

        /// <summary>LZ4 compression</summary>
        public const int Lz4 = 3;

        /// <summary>ZSTD compression</summary>
        public const int Zstd = 4;

        /// <summary>Bit mask to extract compression type from attributes</summary>
        public const int Mask = 0x07;
    }

    // ===================================================================
    // RecordBatch Attributes (bits beyond compression)
    // ===================================================================

    /// <summary>
    /// RecordBatch attribute flags and masks
    /// </summary>
    public static class Attributes
    {
        /// <summary>Bit 3: Timestamp type (0 = CreateTime, 1 = LogAppendTime)</summary>
        public const int TimestampTypeBit = 0x08;

        /// <summary>Bit 4: Is transactional (1 if part of a transaction)</summary>
        public const int IsTransactionalBit = 0x10;

        /// <summary>Bit 5: Is control batch (1 if control batch, e.g., transaction markers)</summary>
        public const int IsControlBatchBit = 0x20;

        /// <summary>Check if batch is transactional</summary>
        public static bool IsTransactional(short attributes) => (attributes & IsTransactionalBit) != 0;

        /// <summary>Check if batch is a control batch</summary>
        public static bool IsControlBatch(short attributes) => (attributes & IsControlBatchBit) != 0;

        /// <summary>Check if timestamp type is LogAppendTime</summary>
        public static bool IsLogAppendTime(short attributes) => (attributes & TimestampTypeBit) != 0;
    }

    // ===================================================================
    // Transaction Control Record Types
    // ===================================================================

    /// <summary>
    /// Control record types for transaction markers
    /// </summary>
    public static class ControlRecordType
    {
        /// <summary>Transaction abort marker</summary>
        public const short Abort = 0;

        /// <summary>Transaction commit marker</summary>
        public const short Commit = 1;
    }

    // ===================================================================
    // Transaction States
    // ===================================================================

    /// <summary>
    /// Transaction state machine states
    /// </summary>
    public enum TransactionState
    {
        /// <summary>No active transaction</summary>
        Empty = 0,

        /// <summary>Transaction is ongoing</summary>
        Ongoing = 1,

        /// <summary>Preparing to commit</summary>
        PrepareCommit = 2,

        /// <summary>Preparing to abort</summary>
        PrepareAbort = 3,

        /// <summary>Transaction completed (commit)</summary>
        CompleteCommit = 4,

        /// <summary>Transaction completed (abort)</summary>
        CompleteAbort = 5,

        /// <summary>Transaction is dead (fenced or expired)</summary>
        Dead = 6
    }

    // ===================================================================
    // Producer ID Constants
    // ===================================================================

    /// <summary>
    /// Producer ID related constants
    /// </summary>
    public static class Producer
    {
        /// <summary>No producer ID assigned</summary>
        public const long NoProducerId = -1;

        /// <summary>No producer epoch</summary>
        public const short NoProducerEpoch = -1;

        /// <summary>No sequence number</summary>
        public const int NoSequence = -1;
    }

    // ===================================================================
    // Segment & Log Configuration
    // ===================================================================

    /// <summary>
    /// Default configuration values for log segments
    /// </summary>
    public static class Defaults
    {
        /// <summary>Default maximum segment file size (1 GB)</summary>
        public const long MaxSegmentSize = 1024L * 1024 * 1024;

        /// <summary>Default segment roll interval (7 days in milliseconds)</summary>
        public const long SegmentRollMs = 7L * 24 * 60 * 60 * 1000;
    }

    // ===================================================================
    // Network Ports
    // ===================================================================

    /// <summary>
    /// Default network port constants
    /// </summary>
    public static class Ports
    {
        /// <summary>Default Kafka protocol port</summary>
        public const int Kafka = 9092;

        /// <summary>Default gRPC port for native protocol</summary>
        public const int Grpc = 9093;

        /// <summary>Default replication port for broker-to-broker communication</summary>
        public const int Replication = 10092;
    }

    // ===================================================================
    // Buffer Sizes
    // ===================================================================

    /// <summary>
    /// Standard buffer size constants
    /// </summary>
    public static class BufferSizes
    {
        /// <summary>Small buffer (4 KB)</summary>
        public const int Small = 4 * 1024;

        /// <summary>Medium buffer (64 KB)</summary>
        public const int Medium = 64 * 1024;

        /// <summary>Large buffer (1 MB)</summary>
        public const int Large = 1024 * 1024;

        /// <summary>Extra large buffer (16 MB)</summary>
        public const int XLarge = 16 * 1024 * 1024;

        /// <summary>Default socket send/receive buffer (100 KB)</summary>
        public const int SocketDefault = 100 * 1024;

        /// <summary>Maximum request/message size (100 MB)</summary>
        public const int MaxRequest = 100 * 1024 * 1024;

        /// <summary>Default fetch max bytes (1 MB)</summary>
        public const int FetchDefault = 1024 * 1024;

        /// <summary>Default producer batch size (16 KB)</summary>
        public const int ProducerBatch = 16 * 1024;
    }

    // ===================================================================
    // Timeouts and Intervals
    // ===================================================================

    /// <summary>
    /// Timeout and interval constants in milliseconds
    /// </summary>
    public static class Timeouts
    {
        /// <summary>Default fetch max wait (500 ms)</summary>
        public const int FetchMaxWaitMs = 500;

        /// <summary>Heartbeat interval (3 seconds)</summary>
        public const int HeartbeatIntervalMs = 3000;

        /// <summary>Heartbeat timeout (10 seconds)</summary>
        public const int HeartbeatTimeoutMs = 10000;

        /// <summary>Request timeout (30 seconds)</summary>
        public const int RequestTimeoutMs = 30000;

        /// <summary>Replica lag time max (30 seconds)</summary>
        public const int ReplicaLagTimeMaxMs = 30000;

        /// <summary>Auto-commit interval (5 seconds)</summary>
        public const int AutoCommitIntervalMs = 5000;

        /// <summary>Shutdown timeout (30 seconds)</summary>
        public const int ShutdownTimeoutSeconds = 30;

        /// <summary>Leader imbalance check interval (5 minutes)</summary>
        public const int LeaderImbalanceCheckSeconds = 300;

        /// <summary>Tiering background interval (5 minutes)</summary>
        public const int TieringIntervalSeconds = 300;
    }

    // ===================================================================
    // Retention Defaults
    // ===================================================================

    /// <summary>
    /// Log retention default values
    /// </summary>
    public static class Retention
    {
        /// <summary>Default log retention (7 days in hours)</summary>
        public const int DefaultHours = 168;

        /// <summary>Local retention for tiered storage (24 hours)</summary>
        public const int LocalRetentionHours = 24;

        /// <summary>Tiering lag before upload (1 hour)</summary>
        public const int TieringLagHours = 1;

        /// <summary>Unlimited retention (-1)</summary>
        public const long Unlimited = -1;
    }

    // ===================================================================
    // Raft Consensus
    // ===================================================================

    /// <summary>
    /// Raft consensus protocol constants
    /// </summary>
    public static class Raft
    {
        /// <summary>Minimum election timeout (150 ms)</summary>
        public const int ElectionTimeoutMinMs = 150;

        /// <summary>Maximum election timeout (300 ms)</summary>
        public const int ElectionTimeoutMaxMs = 300;

        /// <summary>Heartbeat interval (50 ms)</summary>
        public const int HeartbeatIntervalMs = 50;
    }

    // ===================================================================
    // Replication API Keys (Internal Protocol)
    // ===================================================================

    /// <summary>
    /// Internal replication API keys for broker-to-broker communication
    /// </summary>
    public static class ReplicationApiKeys
    {
        /// <summary>Heartbeat request between brokers</summary>
        public const short Heartbeat = 100;

        /// <summary>Raft RequestVote RPC</summary>
        public const short RequestVote = 101;

        /// <summary>Raft AppendEntries RPC</summary>
        public const short AppendEntries = 102;
    }
}
