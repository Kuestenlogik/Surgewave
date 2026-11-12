using System.Net.Security;
using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Health;

/// <summary>
/// <c>surgewave-broker health [ready|live]</c>: asks the broker running in the same container for its health and
/// exits 0 when it answers healthy, 1 otherwise.
/// </summary>
/// <remarks>
/// The container image holds the runtime and the broker, no HTTP client, so a container health check — Docker's
/// <c>HEALTHCHECK</c>, a Compose <c>healthcheck</c> — has nothing else to ask with. The probe reads the endpoint from
/// the configuration the broker itself starts with, so the health check names no port a second time. Kubernetes
/// probes the endpoints directly and does not need it.
/// </remarks>
public static class HealthProbe
{
    /// <summary>The first command-line argument that makes the process a probe instead of a broker.</summary>
    public const string Verb = "health";

    /// <summary>The check asked for when none is named: whether the broker serves clients.</summary>
    public const string Ready = "ready";

    /// <summary>The check that only says the process is running.</summary>
    public const string Live = "live";

    /// <summary>
    /// Probes the check named by the first of <paramref name="args"/> and returns the process exit code.
    /// </summary>
    public static async Task<int> RunAsync(string[] args, TextWriter output, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);

        var check = args.Length > 0 ? args[0] : Ready;
        if (check is not (Ready or Live))
        {
            await output.WriteLineAsync($"Unknown health check '{check}'; it is '{Ready}' or '{Live}'.");
            return 2;
        }

        var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
            ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? "Production";
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true)
            .AddEnvironmentVariables()
            .AddCommandLine(args.Length > 1 ? args[1..] : [])
            .Build();

        var address = new Uri(Endpoint(configuration), $"/health/{check}");
        using var handler = new SocketsHttpHandler();
        // The broker's own endpoint over loopback: its certificate names the host clients reach it by, not localhost,
        // so the name may differ. The chain has to hold as for any client.
        handler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, errors) =>
            errors is SslPolicyErrors.None or SslPolicyErrors.RemoteCertificateNameMismatch;
        using var client = new HttpClient(handler);
        try
        {
            using var response = await client.GetAsync(address, cancellationToken);
            await output.WriteLineAsync($"{address}: {(int)response.StatusCode}");
            return response.IsSuccessStatusCode ? 0 : 1;
        }
        catch (HttpRequestException ex)
        {
            await output.WriteLineAsync($"{address}: {ex.Message}");
            return 1;
        }
    }

    /// <summary>
    /// The base address of the broker's gRPC / REST endpoint as this process reaches it: the endpoint the broker binds
    /// with <paramref name="configuration"/>, on loopback where it binds every interface.
    /// </summary>
    public static Uri Endpoint(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var grpcPort = configuration.GetValue("Surgewave:GrpcPort", defaultValue: 9093);
        // As the broker decides it: GrpcUseTls replaces the configured endpoint with https on GrpcPort.
        var url = configuration.GetValue("Surgewave:GrpcUseTls", defaultValue: false)
            ? $"https://*:{grpcPort}"
            : configuration["Kestrel:Endpoints:Grpc:Url"] ?? $"http://*:{grpcPort}";

        // Kestrel's wildcards are no host a URI can carry.
        var bound = new Uri(url.Replace("://*", "://localhost", StringComparison.Ordinal)
            .Replace("://+", "://localhost", StringComparison.Ordinal));
        var loopback = System.Net.IPAddress.TryParse(bound.Host.Trim('[', ']'), out var ip)
            && (ip.Equals(System.Net.IPAddress.Any) || ip.Equals(System.Net.IPAddress.IPv6Any));
        return loopback ? new UriBuilder(bound) { Host = "localhost" }.Uri : bound;
    }
}
