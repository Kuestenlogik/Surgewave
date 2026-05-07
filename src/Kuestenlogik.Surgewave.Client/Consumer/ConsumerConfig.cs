using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Client.Dlq;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Consumer configuration
/// </summary>
public sealed record ConsumerConfig : IValidatableConfig
{
    [Required]
    [MinLength(1)]
    public required string BootstrapServers { get; init; }

    public string? GroupId { get; init; }
    public string? ClientId { get; init; }
    public bool EnableAutoCommit { get; init; } = true;

    [Range(1, int.MaxValue)]
    public int AutoCommitIntervalMs { get; init; } = 5000;

    [Range(1, int.MaxValue)]
    public int FetchMinBytes { get; init; } = 1;

    [Range(0, int.MaxValue)]
    public int FetchMaxWaitMs { get; init; } = 500;

    [Range(1, 100_000)]
    public int MaxPollRecords { get; init; } = 500;

    [RegularExpression("^(earliest|latest|none)$",
        ErrorMessage = "AutoOffsetReset must be 'earliest', 'latest' or 'none'.")]
    public string AutoOffsetReset { get; init; } = "latest"; // earliest, latest

    /// <summary>
    /// Dead Letter Queue configuration. Set to enable DLQ routing for handler failures.
    /// </summary>
    public ConsumerDlqConfig? DlqConfig { get; init; }

    /// <summary>
    /// Optional TLS client-cert configuration. When set, the consumer
    /// wraps its <see cref="System.Net.Sockets.NetworkStream"/> with
    /// <see cref="System.Net.Security.SslStream"/> and presents the
    /// PEM-encoded cert during the handshake.
    /// </summary>
    public SslOptions? Ssl { get; init; }

    /// <summary>
    /// Optional SASL credentials. When set, the consumer runs the
    /// Kafka SASL handshake immediately after the TCP/TLS connect,
    /// before the first fetch frame.
    /// </summary>
    public SaslOptions? Sasl { get; init; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
