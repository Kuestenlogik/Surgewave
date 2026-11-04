using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// The address a broker binds so a client in a container reaches it: a broker advertises the address it binds, and
/// loopback inside a container is the container (#195).
/// </summary>
internal static class ContainerReachableHost
{
    /// <summary>
    /// The address of the interface the host routes outbound traffic through. Connecting a UDP socket sends
    /// nothing; it only selects the route. Without a route, the first up IPv4 interface that is not loopback.
    /// </summary>
    public static IPAddress? OutboundIPv4Address()
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9));
            if (socket.LocalEndPoint is IPEndPoint { Address: var address } && !IPAddress.IsLoopback(address))
                return address;
        }
        catch (SocketException)
        {
            // No route: fall back to the interfaces.
        }

        return NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Select(a => a.Address)
            .FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork);
    }
}
