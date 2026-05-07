using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Protocol.Quic;

/// <summary>
/// Configuration for the raw QUIC transport adapter. Bound from the "Surgewave:Quic" section.
/// </summary>
public sealed class QuicConfig : IValidatableConfig
{
    public const string SectionName = "Surgewave:Quic";

    /// <summary>Enable the QUIC listener. Default: false.</summary>
    public bool Enabled { get; set; }

    /// <summary>UDP port to listen on. Default: 9094.</summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 9094;

    /// <summary>Maximum concurrent QUIC connections. Default: 1000.</summary>
    [Range(1, int.MaxValue)]
    public int MaxConnections { get; set; } = 1000;

    /// <summary>Maximum bidirectional streams allowed per QUIC connection. Default: 256.</summary>
    [Range(1, int.MaxValue)]
    public int MaxStreamsPerConnection { get; set; } = 256;

    /// <summary>Idle timeout in seconds before an inactive QUIC connection is closed. Default: 60.</summary>
    [Range(1, int.MaxValue)]
    public int IdleTimeoutSeconds { get; set; } = 60;

    /// <summary>
    /// Path to a PKCS#12 (.pfx) certificate file. When empty, a self-signed
    /// certificate is generated for dev/localhost use.
    /// </summary>
    public string CertificatePath { get; set; } = "";

    /// <summary>Password for the PKCS#12 certificate file. Empty when the file is unprotected.</summary>
    public string CertificatePassword { get; set; } = "";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
