using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// A Connect worker creates its config, offsets and status topics itself, compacted, as Kafka Connect does (#215):
/// with the broker's topic auto-creation off, its start waited for a config topic nobody created, and a broker starting
/// Connect in-process never became ready.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class ConnectInternalTopicsTests
{
    [Fact(Timeout = 60000)]
    public async Task AWorkerStartsWithoutAutoCreationAndCreatesItsTopicsCompacted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(false)
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync(cancellationToken);
        var client = await SurgewaveClient.Create(runtime.BootstrapServers).UseSurgewaveProtocol().BuildAsync(cancellationToken);
        await client.ConnectAsync(cancellationToken);
        var config = new ConnectWorkerConfig { BootstrapServers = runtime.BootstrapServers, OffsetsTopicPartitions = 3, StatusTopicPartitions = 2 };
        await using var worker = new ConnectWorker(config, client, NullLogger<ConnectWorker>.Instance, new ConnectWorkerServices(), ownsClient: true);

        await worker.StartAsync(cancellationToken);

        foreach (var (topic, partitions) in new[] { (config.ConfigTopic, 1), (config.OffsetsTopic, 3), (config.StatusTopic, 2) })
        {
            var metadata = runtime.LogManager.GetTopicMetadata(topic);
            Assert.NotNull(metadata);
            Assert.Equal(partitions, metadata.PartitionCount);
            Assert.Equal("compact", metadata.Config["cleanup.policy"]);
        }
    }
}
