namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// One PushTelemetry observation, bundled for the ingestor pipeline so future
/// fields (e.g. authenticated principal, broker id) can be added without
/// changing every implementation's signature.
/// </summary>
public sealed record TelemetryPushEvent
{
    /// <summary>Stable client identifier from <c>GetTelemetrySubscriptions</c>.</summary>
    public required Guid ClientInstanceId { get; init; }

    /// <summary>Application-supplied <c>client.id</c> — may be empty.</summary>
    public required string ClientId { get; init; }

    /// <summary>Subscription id the client believes it's pushing under.</summary>
    public int SubscriptionId { get; init; }

    /// <summary>Compression codec applied to <see cref="MetricsPayload"/> (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd).</summary>
    public sbyte CompressionType { get; init; }

    /// <summary>Raw OTLP MetricsData blob, possibly compressed per <see cref="CompressionType"/>.</summary>
    public required ReadOnlyMemory<byte> MetricsPayload { get; init; }

    /// <summary>Client signalled "I'm shutting down" — last push for this instance.</summary>
    public bool Terminating { get; init; }

    /// <summary>UTC moment the broker received the push.</summary>
    public DateTimeOffset ReceivedAt { get; init; } = DateTimeOffset.UtcNow;
}
