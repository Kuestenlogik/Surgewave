namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// Broker-side configuration for KIP-714 client telemetry. Bound from
/// <c>Surgewave:Telemetry</c>. The default keeps the current "ignore me" stub
/// shape — empty subscription, 5-minute push backoff — so Confluent.Kafka
/// 2.x clients don't suddenly start pushing OTLP payloads at the broker
/// after an upgrade. Operators flip <see cref="Enabled"/> to <c>true</c>
/// and the broker advertises a real subscription with the configured
/// push interval.
/// </summary>
public sealed class ClientTelemetryConfig
{
    /// <summary>
    /// Master switch. When false, the broker advertises an empty
    /// subscription set and discards any pushed payloads — exactly the
    /// pre-G9 stub behaviour. When true, ingestion is on.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// How often clients should push their metric set. Default 30 s — fast
    /// enough to surface client-side regressions in operator dashboards
    /// without overwhelming the broker. Set lower for noisy debug; higher
    /// to reduce bandwidth.
    /// </summary>
    public int PushIntervalMs { get; set; } = 30_000;

    /// <summary>
    /// Maximum bytes per <c>PushTelemetry</c> payload the broker accepts.
    /// 1 MiB by default — a comfortable ceiling for the typical librdkafka
    /// metric set.
    /// </summary>
    public int MaxBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Subscribed metric prefixes. Confluent / Kafka tooling expects
    /// dotted prefixes like <c>org.apache.kafka.producer.*</c>. Empty list
    /// means "all metrics the client emits" — fine for most setups.
    /// </summary>
    public List<string> RequestedMetrics { get; set; } = [];

    /// <summary>
    /// Mirror received OTLP-encoded telemetry payloads to a Surgewave topic so a
    /// downstream observability pipeline can decode them with standard OTLP
    /// tooling. Default <c>false</c>: the broker logs counts only and does
    /// not retain the raw bytes. The topic is auto-created on first push.
    /// </summary>
    public bool TopicSinkEnabled { get; set; }

    /// <summary>
    /// Internal topic name when <see cref="TopicSinkEnabled"/> is on.
    /// Default <c>_client_telemetry</c> — the underscore prefix matches
    /// Surgewave's other internal topics (<c>_audit_events</c>,
    /// <c>__cluster_metadata</c>) so it's hidden from default consumer
    /// listings.
    /// </summary>
    public string TopicName { get; set; } = "_client_telemetry";

    /// <summary>
    /// How long the audit topic retains telemetry records. Default 24 h —
    /// telemetry is for live debugging, not long-term audit; the audit
    /// topic itself defaults to 7 days.
    /// </summary>
    public long RetentionMs { get; set; } = 24 * 60 * 60 * 1000L;
}
