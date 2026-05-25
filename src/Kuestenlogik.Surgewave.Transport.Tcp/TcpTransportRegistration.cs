using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Transport.Tcp;

/// <summary>
/// Registration helper for TCP transport.
/// </summary>
public static class TcpTransportRegistration
{
    private static bool _registered;

    /// <summary>
    /// Register the TCP transport with the factory.
    /// Called automatically via module initializer.
    /// </summary>
    public static void Register()
    {
        if (_registered) return;
        _registered = true;

        SurgewaveTransportFactory.RegisterTcpTransport(options => new TcpTransport(options));
        PeerTransportFactory.Register(TcpPeerTransport.TransportName, () => new TcpPeerTransport());
    }

    [ModuleInitializer]
    [SuppressMessage("Usage", "CA2255:The 'ModuleInitializer' attribute should not be used in libraries", Justification = "Intentional auto-registration pattern")]
    internal static void Initialize()
    {
        Register();
    }
}
