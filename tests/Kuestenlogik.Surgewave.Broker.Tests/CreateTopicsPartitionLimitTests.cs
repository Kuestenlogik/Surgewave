using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// One CreateTopics request creates at most MaxPartitionsPerCreateTopicsRequest partitions, its topics together
/// (#133, Kafka e74637ae04). Without the limit a request for int.MaxValue partitions tried to create them.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CreateTopicsPartitionLimitTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-createtopics-limit-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly QuotaManager _quotaManager;
    private readonly TopicAdminHandler _handler;

    public CreateTopicsPartitionLimitTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var config = new BrokerConfig { BrokerId = 1, DefaultNumPartitions = 4, MaxPartitionsPerCreateTopicsRequest = 10 };
        _quotaManager = new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance);
        _handler = new TopicAdminHandler(config, _logManager, _quotaManager, auditLogger: null, NullLogger<TopicAdminHandler>.Instance);
    }

    public void Dispose()
    {
        _quotaManager.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task PartitionCountsThatOverflowAnIntFailTheWholeRequest()
    {
        var response = await CreateAsync(Topic("foo", int.MaxValue), Topic("bar", int.MaxValue));

        Assert.All(response.Topics, t => Assert.Equal(ErrorCode.PolicyViolation, t.ErrorCode));
        Assert.Null(_logManager.GetTopicMetadata("foo"));
        Assert.Null(_logManager.GetTopicMetadata("bar"));
    }

    [Fact]
    public async Task TheLimitCountsDefaultsAndAssignmentsAndHoldsForValidateOnly()
    {
        // 4 (default for -1) + 3 (assignments) + 4 = 11 > 10
        var assigned = new CreateTopicsRequest.TopicToCreate
        {
            Name = "assigned",
            NumPartitions = -1,
            ReplicationFactor = -1,
            Assignments = [.. Enumerable.Range(0, 3).Select(p => new CreateTopicsRequest.ReplicaAssignment { Partition = p, BrokerIds = [1] })],
        };

        var response = await CreateAsync(validateOnly: true, Topic("defaulted", -1), assigned, Topic("explicit", 4));

        Assert.All(response.Topics, t => Assert.Equal(ErrorCode.PolicyViolation, t.ErrorCode));
    }

    [Fact]
    public async Task ARequestAtTheLimitCreatesItsTopics()
    {
        var response = await CreateAsync(Topic("six", 6), Topic("four", 4));

        Assert.All(response.Topics, t => Assert.Equal(ErrorCode.None, t.ErrorCode));
        Assert.NotNull(_logManager.GetTopicMetadata("six"));
    }

    private static CreateTopicsRequest.TopicToCreate Topic(string name, int partitions) =>
        new() { Name = name, NumPartitions = partitions, ReplicationFactor = 1 };

    private Task<CreateTopicsResponse> CreateAsync(params CreateTopicsRequest.TopicToCreate[] topics) =>
        CreateAsync(validateOnly: false, topics);

    private async Task<CreateTopicsResponse> CreateAsync(bool validateOnly, params CreateTopicsRequest.TopicToCreate[] topics) =>
        Assert.IsType<CreateTopicsResponse>(await _handler.HandleAsync(new CreateTopicsRequest
        {
            ApiKey = ApiKey.CreateTopics,
            ApiVersion = 7,
            CorrelationId = 1,
            ClientId = "admin",
            TimeoutMs = 30_000,
            ValidateOnly = validateOnly,
            Topics = [.. topics],
        }, new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" }, CancellationToken.None));
}
