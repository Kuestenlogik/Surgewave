using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Core.Telemetry;

/// <summary>
/// Configuration for OpenTelemetry telemetry (metrics + tracing).
/// </summary>
public sealed class TelemetryConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Telemetry";

    /// <summary>
    /// Service name for telemetry identification.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ServiceName { get; set; } = "Kuestenlogik.Surgewave";

    /// <summary>
    /// Service version for telemetry identification.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ServiceVersion { get; set; } = "1.0.0";

    /// <summary>
    /// OTLP exporter configuration.
    /// </summary>
    public OtlpConfig Otlp { get; set; } = new();

    /// <summary>
    /// Prometheus exporter configuration.
    /// </summary>
    public PrometheusConfig Prometheus { get; set; } = new();

    /// <summary>
    /// Tracing configuration.
    /// </summary>
    public TracingConfig Tracing { get; set; } = new();

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));
        errors.AddRange(Otlp.Validate());
        errors.AddRange(Prometheus.Validate());
        errors.AddRange(Tracing.Validate());
        return errors;
    }
}

/// <summary>
/// OTLP (OpenTelemetry Protocol) exporter configuration.
/// </summary>
public sealed class OtlpConfig : IValidatableConfig
{
    /// <summary>
    /// Whether OTLP export is enabled.
    /// Can be auto-enabled by setting OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// OTLP collector endpoint (e.g., http://localhost:4317 for gRPC, http://localhost:4318 for HTTP).
    /// </summary>
    [Required]
    [Url]
    public string Endpoint { get; set; } = "http://localhost:4317";

    /// <summary>
    /// OTLP protocol: "Grpc" or "HttpProtobuf".
    /// </summary>
    [RegularExpression("^(Grpc|HttpProtobuf)$",
        ErrorMessage = "Protocol must be 'Grpc' or 'HttpProtobuf'.")]
    public string Protocol { get; set; } = "Grpc";

    /// <summary>
    /// Optional headers for authentication (e.g., "Authorization=Bearer token").
    /// </summary>
    public string? Headers { get; set; }

    /// <summary>
    /// Export timeout in milliseconds.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int TimeoutMs { get; set; } = 30000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

/// <summary>
/// Prometheus exporter configuration.
/// </summary>
public sealed class PrometheusConfig : IValidatableConfig
{
    /// <summary>
    /// Whether Prometheus scraping endpoint is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Path for Prometheus scraping endpoint.
    /// </summary>
    [Required]
    [RegularExpression("^/.*", ErrorMessage = "Path must start with '/'.")]
    public string Path { get; set; } = "/metrics";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

/// <summary>
/// Tracing configuration.
/// </summary>
public sealed class TracingConfig : IValidatableConfig
{
    /// <summary>
    /// Sampling ratio (0.0 to 1.0). 1.0 means sample all traces.
    /// </summary>
    [Range(0.0, 1.0)]
    public double SamplingRatio { get; set; } = 1.0;

    /// <summary>
    /// Whether to include ASP.NET Core HTTP instrumentation.
    /// </summary>
    public bool IncludeAspNetCore { get; set; } = true;

    /// <summary>
    /// Whether to include gRPC client instrumentation.
    /// </summary>
    public bool IncludeGrpc { get; set; } = true;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
