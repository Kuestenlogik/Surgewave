using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Verifies that <see cref="StreamsConfig"/> rejects invalid values via the
/// IValidatableConfig contract introduced in the configuration validation refactoring.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class StreamsConfigValidationTests
{
    [Fact]
    public void Validate_DefaultValidConfig_ReturnsNoErrors()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092"
        };

        Assert.Empty(config.Validate());
    }

    [Fact]
    public void Validate_NumStreamThreadsZero_ReportsError()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 0
        };

        Assert.Contains(config.Validate(), e => e.Contains(nameof(StreamsConfig.NumStreamThreads)));
    }

    [Fact]
    public void Validate_BadAutoOffsetReset_ReportsError()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = "middle"
        };

        Assert.Contains(config.Validate(), e => e.Contains(nameof(StreamsConfig.AutoOffsetReset)));
    }

    [Fact]
    public void Validate_ExactlyOnceWithoutIdempotence_ReportsCrossPropertyError()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            ProcessingGuarantee = ProcessingGuarantee.ExactlyOnce,
            EnableIdempotence = false
        };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains(nameof(StreamsConfig.EnableIdempotence)));
    }

    [Fact]
    public void Validate_ExactlyOnceWithIdempotence_NoError()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            ProcessingGuarantee = ProcessingGuarantee.ExactlyOnce,
            EnableIdempotence = true
        };

        Assert.Empty(config.Validate());
    }

    [Fact]
    public void Validate_ZeroPollTimeout_ReportsError()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            PollTimeout = TimeSpan.Zero
        };

        Assert.Contains(config.Validate(), e => e.Contains(nameof(StreamsConfig.PollTimeout)));
    }
}
