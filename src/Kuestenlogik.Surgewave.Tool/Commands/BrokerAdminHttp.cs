namespace Kuestenlogik.Surgewave.Cli.Commands;

/// <summary>
/// Factory for HttpClients that talk to the broker's admin REST API.
/// The API runs on the Kestrel port (default 9093, https) — NOT on the Kafka
/// bootstrap port. The previous per-command pattern built
/// <c>http://{host}:{kafka-port}</c>, which never reached the API at all.
/// For loopback hosts the (self-signed) dev certificate is accepted; remote
/// hosts get full certificate validation.
/// </summary>
public static class BrokerAdminHttp
{
    /// <summary>Default Kestrel port of the broker admin API.</summary>
    public const int DefaultApiPort = 9093;

    /// <summary>
    /// Creates a client for the admin REST API of the broker at
    /// <paramref name="bootstrapHost"/>. The bootstrap port is intentionally
    /// ignored — the admin API always runs on the Kestrel port.
    /// </summary>
    public static HttpClient Create(string bootstrapHost, int apiPort = DefaultApiPort)
    {
#pragma warning disable CA2000 // Ownership wird via disposeHandler:true an den HttpClient übertragen
        var handler = new HttpClientHandler
        {
            CheckCertificateRevocationList = true
        };
#pragma warning restore CA2000
        if (IsLoopback(bootstrapHost))
        {
            handler.ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        }

        // disposeHandler: true — der Handler gehört dem Client und wird mit ihm entsorgt.
        return new HttpClient(handler, disposeHandler: true)
        {
            BaseAddress = new Uri($"https://{bootstrapHost}:{apiPort}"),
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    private static bool IsLoopback(string host) =>
        string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase)
        || host == "127.0.0.1"
        || host == "::1";
}
