using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Runtime.Tests;

/// <summary>
/// An embedded broker that is its whole quorum is handed to its host caught up with its metadata log (#250): what
/// the log held before the restart is there on the line after <c>StartAsync</c>.
/// </summary>
/// <remarks>
/// The broker applies its log only once its Raft node is elected, an election timeout after the start. The timeout
/// here is seconds, so a start that does not wait for it returns long before: the topic is not there, every time.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataCatchUpOnStartTests
{
    // The metadata log's commands are camel-cased JSON.
    private static readonly JsonSerializerOptions CommandJson = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task ABrokerThatIsItsWholeQuorumIsStartedOnceItHasAppliedTheLogItFound()
    {
        var ct = TestContext.Current.CancellationToken;
        var dataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-runtime-catch-up-" + Guid.NewGuid().ToString("N"));
        try
        {
            // What an earlier run left in the broker's metadata log: a topic.
            var raft = new ClusteringConfig { BrokerId = 0, RaftDataDirectory = Path.Combine(dataDirectory, "raft") };
            await new RaftPersistence(NullLogger<RaftPersistence>.Instance, raft).AppendEntriesAsync(
            [
                new RaftLogEntry { Term = 1, Index = 1, CommandType = MetadataCommandType.Noop, Data = [] },
                new RaftLogEntry
                {
                    Term = 1,
                    Index = 2,
                    CommandType = MetadataCommandType.TopicCreated,
                    Data = JsonSerializer.SerializeToUtf8Bytes(
                        new TopicCreatedCommand("payments", Guid.NewGuid(), 1, 1, [], [new PartitionAssignment(0, [0], 1)]),
                        CommandJson),
                },
            ]);

            await using var broker = await SurgewaveRuntime.CreateBuilder()
                .WithBrokerId(0)
                .WithCluster()
                .WithDataDirectory(dataDirectory)
                .WithStorageEngine(StorageEngines.Memory)
                .WithRaftElectionTimeout(1000, 1001)
                .WithShutdownTimeout(2)
                .Build()
                .StartAsync(ct);

            Assert.NotNull(broker.ClusterState!.GetTopic("payments"));
            Assert.True(broker.IsController);
            Assert.True(broker.WaitForMetadataCaughtUpAsync(ct).IsCompletedSuccessfully);
        }
        finally
        {
            try { if (Directory.Exists(dataDirectory)) Directory.Delete(dataDirectory, recursive: true); }
            catch (IOException) { /* a file still held is not a failure */ }
        }
    }
}
