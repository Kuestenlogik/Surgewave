using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Hosting;

/// <summary>
/// The broker's two HTTP endpoints as <c>Surgewave:GrpcUseTls</c> decides them.
/// </summary>
/// <remarks>
/// <para>
/// <c>appsettings.json</c> names two Kestrel endpoints: <c>Grpc</c> on the gRPC port and <c>GrpcHttp3</c> on the port
/// after it. What they serve depends on TLS, because a client and a server agree on HTTP/2 or HTTP/1.1 in the TLS
/// handshake and an endpoint without TLS serves one of the two:
/// </para>
/// <list type="table">
/// <item><term>with TLS</term><description><c>Grpc</c>: gRPC, REST, <c>/health</c> and <c>/metrics</c> over HTTPS,
/// HTTP/1.1 and HTTP/2. <c>GrpcHttp3</c>: the same, and HTTP/3. Both with the certificate of
/// <c>Surgewave:GrpcCertificatePath</c>, or the ASP.NET Core development certificate when none is
/// named.</description></item>
/// <item><term>without TLS</term><description><c>Grpc</c>: REST, <c>/health</c> and <c>/metrics</c> over HTTP/1.1.
/// <c>GrpcHttp3</c>: gRPC over HTTP/2 with prior knowledge, since HTTP/3 needs TLS.</description></item>
/// </list>
/// <para>
/// The setting is the one switch: <c>false</c> yields cleartext and <c>true</c> HTTPS whatever scheme the endpoint
/// URLs are written with, so neither a container image nor a test has to rewrite the endpoints to change it. With
/// the setting absent the Kestrel section stands as written.
/// </para>
/// </remarks>
public static class BrokerEndpoints
{
    /// <summary>The setting that decides whether the endpoints are served over TLS.</summary>
    public const string UseTlsKey = "Surgewave:GrpcUseTls";

    /// <summary>The address of the endpoint on the gRPC port.</summary>
    public const string GrpcUrlKey = "Kestrel:Endpoints:Grpc:Url";

    /// <summary>The address of the second endpoint: HTTP/3 with TLS, gRPC over cleartext HTTP/2 without.</summary>
    public const string SecondUrlKey = "Kestrel:Endpoints:GrpcHttp3:Url";

    private const string GrpcSection = "Kestrel:Endpoints:Grpc";
    private const string SecondSection = "Kestrel:Endpoints:GrpcHttp3";
    private const string Https = "https://";
    private const string Http = "http://";

    /// <summary>
    /// The settings that <c>Surgewave:GrpcUseTls</c> replaces in <paramref name="configuration"/>, by key. Empty when
    /// the setting is absent.
    /// </summary>
    public static IReadOnlyDictionary<string, string> Resolve(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(configuration[UseTlsKey]))
            return settings;

        var grpcPort = configuration.GetValue("Surgewave:GrpcPort", defaultValue: new BrokerConfig().GrpcPort);
        var secondUrl = configuration[SecondUrlKey];

        if (configuration.GetValue<bool>(UseTlsKey))
        {
            settings[GrpcUrlKey] = $"{Https}*:{grpcPort}";
            // A configuration that silenced Kestrel's warning about HTTP/2 without TLS gets its warnings back.
            settings["Logging:LogLevel:Microsoft.AspNetCore.Server.Kestrel"] = "Warning";
            if (secondUrl is not null)
                settings[SecondUrlKey] = WithScheme(secondUrl, Https);

            // One certificate for both endpoints: without it the second one looks for the development certificate
            // and the broker ends with "Unable to configure HTTPS endpoint". An endpoint that names a certificate
            // of its own keeps it.
            var certificatePath = configuration["Surgewave:GrpcCertificatePath"];
            if (!string.IsNullOrEmpty(certificatePath))
            {
                var certificatePassword = configuration["Surgewave:GrpcCertificatePassword"];
                settings[$"{GrpcSection}:Certificate:Path"] = certificatePath;
                if (!string.IsNullOrEmpty(certificatePassword))
                    settings[$"{GrpcSection}:Certificate:Password"] = certificatePassword;

                if (secondUrl is not null && !configuration.GetSection($"{SecondSection}:Certificate").Exists())
                {
                    settings[$"{SecondSection}:Certificate:Path"] = certificatePath;
                    if (!string.IsNullOrEmpty(certificatePassword))
                        settings[$"{SecondSection}:Certificate:Password"] = certificatePassword;
                }
            }

            return settings;
        }

        settings[GrpcUrlKey] = WithScheme(configuration[GrpcUrlKey] ?? $"{Http}*:{grpcPort}", Http);
        // Kestrel serves HTTP/1.1 on a cleartext endpoint that names both and warns at every start that it does;
        // the endpoint says what it serves. One set to HTTP/2 alone is left as it is.
        if (!IsProtocol(configuration[$"{GrpcSection}:Protocols"], "Http2"))
            settings[$"{GrpcSection}:Protocols"] = "Http1";

        if (secondUrl is not null)
        {
            settings[SecondUrlKey] = WithScheme(secondUrl, Http);
            // HTTP/3 needs TLS. What the endpoint serves instead is gRPC, which the endpoint above cannot.
            var protocols = configuration[$"{SecondSection}:Protocols"];
            if (protocols is null || protocols.Contains("Http3", StringComparison.OrdinalIgnoreCase))
                settings[$"{SecondSection}:Protocols"] = "Http2";
        }

        return settings;
    }

    /// <summary>
    /// Writes what <see cref="Resolve"/> decides into <paramref name="configuration"/>, over every source: the host
    /// binds what the configuration says after this.
    /// </summary>
    public static void Apply(IConfiguration configuration)
    {
        foreach (var (key, value) in Resolve(configuration))
            configuration[key] = value;
    }

    /// <summary>
    /// The port on which the broker serves gRPC over cleartext HTTP/2, or <see langword="null"/> when no endpoint of
    /// <paramref name="configuration"/> does. Read after <see cref="Apply"/>.
    /// </summary>
    public static int? CleartextGrpcPort(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        foreach (var section in (string[])[GrpcSection, SecondSection])
        {
            var url = configuration[$"{section}:Url"];
            if (url is not null
                && url.StartsWith(Http, StringComparison.OrdinalIgnoreCase)
                && IsProtocol(configuration[$"{section}:Protocols"], "Http2")
                && int.TryParse(url[(url.LastIndexOf(':') + 1)..].TrimEnd('/'), out var port))
            {
                return port;
            }
        }

        return null;
    }

    private static bool IsProtocol(string? protocols, string protocol) =>
        string.Equals(protocols, protocol, StringComparison.OrdinalIgnoreCase);

    // Kestrel's wildcard hosts (*, +) are no host a Uri can carry, so the scheme is replaced in the text.
    private static string WithScheme(string url, string scheme)
    {
        var separator = url.IndexOf("://", StringComparison.Ordinal);
        return separator < 0 ? scheme + url : scheme + url[(separator + 3)..];
    }
}
