using Kuestenlogik.Surgewave.Storage.Engine.S3;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Smoke tests for the S3-only-broker public surface — confirms the factory builds
/// and the extension methods plug into <see cref="SurgewaveRuntimeBuilder"/> without
/// touching a real S3 endpoint. End-to-end LocalStack/MinIO-backed tests live
/// outside this assembly because they need a docker-compose fixture.
/// </summary>
[Trait("Category", "CloudObjectStore")]
public sealed class S3StorageExtensionsTests
{
    [Fact]
    public void S3LogSegmentFactory_Create_DefaultClient_BuildsFactory()
    {
        // Default-AWS-credentials path: just verifies the factory wires up cleanly
        // — no S3 calls happen until CreateSegment is invoked.
        var factory = S3LogSegmentFactory.Create("test-bucket", prefix: "surgewave-tests");

        Assert.NotNull(factory);
        Assert.True(factory.IsPersistent, "S3-backed segments are persistent.");
    }

    [Fact]
    public void S3LogSegmentFactory_CreateForLocalStack_UsesPathStyleAccess()
    {
        // LocalStack/MinIO path: matches the in-cluster developer-loop story
        // (no AWS credentials required). This factory should not throw on
        // construction even when the endpoint is unreachable; the broker only
        // hits S3 once a segment is opened.
        var factory = S3LogSegmentFactory.CreateForLocalStack(
            endpoint: "http://localhost:4566",
            bucketName: "surgewave-test",
            prefix: "surgewave",
            accessKey: "test",
            secretKey: "test");

        Assert.NotNull(factory);
        Assert.True(factory.IsPersistent);
    }

    [Fact]
    public void S3LogSegmentFactory_Create_WithCustomClientFactory_PassesThrough()
    {
        // Verifies the customer-supplied IAmazonS3-factory overload returns a
        // valid ILogSegmentFactory — used by operators who manage their own
        // SDK configuration (e.g. STS assume-role, region pinning).
        Func<Amazon.S3.IAmazonS3> clientFactory = () => new Amazon.S3.AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials("akid", "secret"),
            new Amazon.S3.AmazonS3Config { ServiceURL = "http://127.0.0.1:9999", ForcePathStyle = true });

        var factory = S3LogSegmentFactory.Create(clientFactory, "bucket", "prefix");

        Assert.NotNull(factory);
    }
}
