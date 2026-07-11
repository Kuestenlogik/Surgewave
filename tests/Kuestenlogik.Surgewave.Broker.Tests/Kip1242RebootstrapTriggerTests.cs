using System.Reflection;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1242 — at <c>ApiVersionsRequest</c> v5+ the client may supply the
/// <c>ClusterId</c> and <c>NodeId</c> it expects to be talking to. If the
/// broker receiving the request doesn't match, the broker returns
/// <c>REBOOTSTRAP_REQUIRED (129)</c> so the client re-resolves the
/// bootstrap endpoint instead of silently continuing against the wrong
/// cluster (the motivating case: IP reuse after cluster replacement).
///
/// Wire-side coverage (parse + error-code enum) is pinned by
/// <c>Kip1242ApiVersionsV5Tests</c>; this test set pins the broker's
/// handler-side trigger logic against the live <see cref="MetadataApiHandler"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1242RebootstrapTriggerTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly MetadataApiHandler _handler;
    private readonly BrokerConfig _config;

    private const string ClusterId = "surgewave-prod-eu-west-1";
    private const int BrokerId = 7;

    public Kip1242RebootstrapTriggerTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-kip1242-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _config = new BrokerConfig
        {
            DataDirectory = _dataDir,
            BrokerId = BrokerId,
            ClusterId = ClusterId,
        };
        _handler = CreateHandler();
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>
    /// <see cref="MetadataApiHandler"/> has several heavy dependencies that
    /// aren't relevant to ApiVersions; the constructor below instantiates
    /// it with the minimal set needed for the ApiVersions code path. The
    /// reflection-based fallback keeps the test resilient if the ctor
    /// signature drifts (which it has historically).
    /// </summary>
    private MetadataApiHandler CreateHandler()
    {
        // Try to find a MetadataApiHandler ctor we can satisfy. The cheapest
        // is one taking (BrokerConfig, LogManager, ...) — we walk ctors and
        // pick the first one whose parameters we can produce with nulls.
        var ctor = typeof(MetadataApiHandler).GetConstructors()
            .OrderBy(c => c.GetParameters().Length)
            .First();
        var args = ctor.GetParameters()
            .Select(p => ResolveCtorArg(p.ParameterType))
            .ToArray();
        return (MetadataApiHandler)ctor.Invoke(args);
    }

    private object? ResolveCtorArg(Type t)
    {
        if (t == typeof(BrokerConfig)) return _config;
        if (t == typeof(LogManager)) return _logManager;
        if (t.IsValueType) return Activator.CreateInstance(t);
        // NullLogger trick for ILogger<T>
        if (t.IsGenericType && t.GetGenericTypeDefinition().Name.StartsWith("ILogger", StringComparison.Ordinal))
        {
            var loggerType = typeof(NullLogger<>).MakeGenericType(t.GetGenericArguments()[0]);
            return loggerType.GetField("Instance")?.GetValue(null) ?? Activator.CreateInstance(loggerType);
        }
        return null;
    }

    private ApiVersionsRequest BuildRequest(short apiVersion, string? clusterId = null, int nodeId = -1) =>
        new()
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = apiVersion,
            CorrelationId = 1,
            ClientId = "kip1242-test",
            ClientSoftwareName = "test",
            ClientSoftwareVersion = "1.0",
            ClusterId = clusterId,
            NodeId = nodeId,
        };

    private ApiVersionsResponse Invoke(ApiVersionsRequest request)
    {
        // The handler exposes HandleApiVersions as private; the dispatcher
        // is the public Handle method, so route through that.
        var handleMethod = typeof(MetadataApiHandler).GetMethod("HandleApiVersions", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (ApiVersionsResponse)handleMethod.Invoke(_handler, [request])!;
    }

    [Fact]
    public void V5_MatchingClusterIdAndNodeId_ReturnsNone()
    {
        var response = Invoke(BuildRequest(apiVersion: 5, clusterId: ClusterId, nodeId: BrokerId));
        Assert.Equal(ErrorCode.None, response.ErrorCode);
        Assert.NotEmpty(response.ApiVersions);
    }

    [Fact]
    public void V5_NullClusterIdAndDefaultNodeId_ReturnsNone()
    {
        // The default request from a client that doesn't pin identity should
        // pass — both fields are optional at v5.
        var response = Invoke(BuildRequest(apiVersion: 5, clusterId: null, nodeId: -1));
        Assert.Equal(ErrorCode.None, response.ErrorCode);
    }

    [Fact]
    public void V5_ClusterIdMismatch_ReturnsRebootstrapRequired()
    {
        var response = Invoke(BuildRequest(apiVersion: 5, clusterId: "wrong-cluster-name"));
        Assert.Equal(ErrorCode.RebootstrapRequired, response.ErrorCode);
        // KIP-1242 says the client must rebootstrap; the broker doesn't
        // need to publish its api list in the error path.
        Assert.Empty(response.ApiVersions);
    }

    [Fact]
    public void V5_NodeIdMismatch_ReturnsRebootstrapRequired()
    {
        var response = Invoke(BuildRequest(apiVersion: 5, clusterId: ClusterId, nodeId: BrokerId + 99));
        Assert.Equal(ErrorCode.RebootstrapRequired, response.ErrorCode);
    }

    [Fact]
    public void V4_ClusterIdMismatch_IsIgnored()
    {
        // The KIP-1242 fields don't exist on the wire pre-v5. Even if the
        // model carries them, the broker must not act on them at v4.
        var response = Invoke(BuildRequest(apiVersion: 4, clusterId: "wrong-cluster-name"));
        Assert.Equal(ErrorCode.None, response.ErrorCode);
    }
}
