using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class AcceptedSocketConfigurationTests
{
    [Fact]
    public async Task ConfigureAcceptedSocket_SetsNoDelayAndBufferSizes()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            using var client = new TcpClient();
            var connectTask = client.ConnectAsync(IPAddress.Loopback, port);
            using var accepted = await listener.AcceptTcpClientAsync();
            await connectTask;

            // Values stay below typical Linux rmem_max/wmem_max caps (212992) so the
            // kernel does not silently clamp them; reads report >= the requested size
            // (Linux doubles SO_SNDBUF/SO_RCVBUF), hence lower-bound asserts.
            var config = new BrokerConfig
            {
                SocketSendBufferBytes = 64 * 1024,
                SocketReceiveBufferBytes = 96 * 1024
            };
            SurgewaveBroker.ConfigureAcceptedSocket(accepted, config);

            Assert.True(accepted.Client.NoDelay);
            Assert.True(accepted.SendBufferSize >= 64 * 1024);
            Assert.True(accepted.ReceiveBufferSize >= 96 * 1024);
        }
        finally
        {
            listener.Stop();
        }
    }
}
