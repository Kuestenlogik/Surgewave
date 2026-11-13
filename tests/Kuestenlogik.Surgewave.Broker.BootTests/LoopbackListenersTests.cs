using System.Net;
using System.Net.NetworkInformation;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The broker a boot test starts listens on loopback only, with each of its four listeners (#285). A listener on
/// every interface raises a firewall prompt on Windows, and nothing outside this host has to reach a boot test's
/// broker.
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class LoopbackListenersTests
{
    [Fact(Timeout = 180_000)]
    public async Task TheBrokerOfABootTestListensOnLoopbackOnly()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Directory.CreateTempSubdirectory("surgewave-boottest-").FullName;
        try
        {
            await using var broker = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");

            // The ports are the broker's own while it runs, so the machine's listeners on them are the broker's.
            var listeners = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();
            foreach (var (name, port) in new[]
            {
                ("Kafka", broker.KafkaPort),
                ("REST", broker.RestPort),
                ("second HTTP endpoint", broker.SecondEndpointPort),
                ("replication", broker.ReplicationPort),
            })
            {
                var bound = listeners.Where(endpoint => endpoint.Port == port).ToList();
                Assert.True(bound.Count > 0, $"The {name} listener on port {port} is not listening.\n{broker.Dump()}");
                Assert.All(bound, endpoint => Assert.True(IPAddress.IsLoopback(endpoint.Address),
                    $"The {name} listener is bound to {endpoint}, which is not loopback."));
            }
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }
}
