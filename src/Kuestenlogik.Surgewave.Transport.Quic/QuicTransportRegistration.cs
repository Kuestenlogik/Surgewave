using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Transport.Quic;

/// <summary>
/// Registration helper for the QUIC transport. Auto-registered via a module
/// initializer so that referencing the assembly is enough to enable
/// <c>SurgewaveTransportType.Quic</c>.
/// </summary>
public static class QuicTransportRegistration
{
    private static bool _registered;

    public static void Register()
    {
        if (_registered) return;
        _registered = true;

        SurgewaveTransportFactory.RegisterQuicTransport(options =>
        {
            if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
            {
                throw new PlatformNotSupportedException(
                    "QUIC transport requires Windows, Linux, or macOS with msquic.");
            }
            return CreateQuicTransport(options);
        });

        // Literal matches QuicPeerTransport.TransportName — can't reference the
        // const directly here because it lives on a platform-gated class.
        PeerTransportFactory.Register("quic", () =>
        {
            if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
            {
                throw new PlatformNotSupportedException(
                    "QUIC peer transport requires Windows, Linux, or macOS with msquic.");
            }
            return CreateQuicPeerTransport();
        });
    }

    private static ISurgewaveTransport CreateQuicTransport(TransportOptions options)
    {
        // Split into a separate method so the analyzer sees the platform guard above.
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            return new QuicTransport(options);
        }
        throw new PlatformNotSupportedException();
    }

    private static IPeerTransport CreateQuicPeerTransport()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            return new QuicPeerTransport();
        }
        throw new PlatformNotSupportedException();
    }

    [ModuleInitializer]
    [SuppressMessage("Usage", "CA2255:The 'ModuleInitializer' attribute should not be used in libraries", Justification = "Intentional auto-registration pattern (mirrors TcpTransportRegistration).")]
    internal static void Initialize()
    {
        Register();
    }
}
