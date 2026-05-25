using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Client.Producer;

/// <summary>
/// Producer configuration
/// </summary>
public sealed record ProducerConfig : IValidatableConfig
{
    [Required]
    [MinLength(1)]
    public required string BootstrapServers { get; init; }

    public string? ClientId { get; init; }

    [Range(1, int.MaxValue)]
    public int RequestTimeoutMs { get; init; } = 30000;

    [Range(-1, 1)]
    public short RequiredAcks { get; init; } = 1; // 0=none, 1=leader, -1=all replicas

    [Range(1, int.MaxValue)]
    public int BatchSize { get; init; } = 16384;

    [Range(0, int.MaxValue)]
    public int LingerMs { get; init; } = 0;

    [Range(1, int.MaxValue)]
    public int MaxInFlightRequests { get; init; } = 5;

    /// <summary>
    /// Optional TLS client-cert configuration. When set, the producer
    /// wraps its <see cref="System.Net.Sockets.NetworkStream"/> with
    /// <see cref="System.Net.Security.SslStream"/> and presents the
    /// PEM-encoded cert during the handshake.
    /// </summary>
    public SslOptions? Ssl { get; init; }

    /// <summary>
    /// Optional SASL credentials. When set, the producer runs the
    /// Kafka SASL handshake (<c>SaslHandshakeRequest</c> +
    /// <c>SaslAuthenticateRequest</c>) immediately after the TCP/TLS
    /// connect, before the first produce frame.
    /// </summary>
    public SaslOptions? Sasl { get; init; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
