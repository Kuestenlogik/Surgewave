namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Protocol type for client connections.
/// </summary>
public enum ProtocolType
{
    /// <summary>
    /// Surgewave Native protocol - optimized for maximum performance.
    /// Supports advanced features like SharedMemory transport, batching presets,
    /// and handler-based message dispatch.
    /// </summary>
    SurgewaveNative,

    /// <summary>
    /// Kafka-compatible protocol for interoperability.
    /// Use when connecting to real Kafka clusters or when Kafka wire protocol
    /// compatibility is required.
    /// </summary>
    Kafka,

    /// <summary>
    /// Auto-detect protocol based on broker capabilities.
    /// Tries Surgewave Native first, falls back to Kafka if not supported.
    /// </summary>
    Auto
}
