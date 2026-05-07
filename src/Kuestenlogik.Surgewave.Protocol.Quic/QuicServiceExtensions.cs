using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Quic;

/// <summary>
/// DI registration for the raw QUIC transport adapter.
/// </summary>
public static class QuicServiceExtensions
{
    /// <summary>
    /// Adds the QUIC transport adapter as a hosted service.
    /// The adapter only listens when Surgewave:Quic:Enabled is true (default: false).
    /// </summary>
    public static IServiceCollection AddSurgewaveQuic(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<QuicConfig>(configuration.GetSection(QuicConfig.SectionName));

        // QuicBrokerAdapter uses System.Net.Quic which is only supported on
        // Windows 11+/Server 2022+, Linux with libmsquic, and macOS. On any
        // other platform, skip registration — the adapter would fail to start
        // anyway via QuicListener.IsSupported.
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            services.AddHostedService<QuicBrokerAdapter>();
        }

        return services;
    }
}
