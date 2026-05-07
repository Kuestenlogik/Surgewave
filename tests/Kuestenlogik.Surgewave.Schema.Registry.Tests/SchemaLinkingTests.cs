using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Tests for cross-cluster schema linking: config, state, patterns, metrics, and conflict resolution.
/// </summary>
public sealed class SchemaLinkingTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;

    public SchemaLinkingTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-schema-linking-tests-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public void SchemaLinkingConfig_Defaults()
    {
        var config = new SchemaLinkingConfig();

        Assert.False(config.Enabled);
        Assert.Empty(config.RemoteRegistries);
        Assert.Equal(SchemaSyncMode.Bidirectional, config.SyncMode);
        Assert.Equal(30, config.SyncIntervalSeconds);
        Assert.Single(config.SubjectPatterns);
        Assert.Equal("*", config.SubjectPatterns[0]);
        Assert.True(config.SyncCompatibilityConfig);
        Assert.Equal(ConflictResolution.HighestVersion, config.ConflictResolution);
    }

    [Fact]
    public void SchemaSyncMode_AllValues()
    {
        var values = Enum.GetValues<SchemaSyncMode>();

        Assert.Equal(3, values.Length);
        Assert.Contains(SchemaSyncMode.Export, values);
        Assert.Contains(SchemaSyncMode.Import, values);
        Assert.Contains(SchemaSyncMode.Bidirectional, values);

        _output.WriteLine($"SchemaSyncMode values: {string.Join(", ", values)}");
    }

    [Fact]
    public void ConflictResolution_AllValues()
    {
        var values = Enum.GetValues<ConflictResolution>();

        Assert.Equal(3, values.Length);
        Assert.Contains(ConflictResolution.HighestVersion, values);
        Assert.Contains(ConflictResolution.LocalWins, values);
        Assert.Contains(ConflictResolution.RemoteWins, values);

        _output.WriteLine($"ConflictResolution values: {string.Join(", ", values)}");
    }

    [Fact]
    public void SchemaLink_SyncedState()
    {
        var link = new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "us-east",
            TargetCluster = "eu-west",
            SourceVersion = 3,
            TargetVersion = 3,
            LastSyncedAt = DateTimeOffset.UtcNow
        };

        Assert.Equal("orders-value", link.Subject);
        Assert.Equal("us-east", link.SourceCluster);
        Assert.Equal("eu-west", link.TargetCluster);
        Assert.Equal(3, link.SourceVersion);
        Assert.Equal(3, link.TargetVersion);
        Assert.Equal(SchemaSyncStatus.Synced, link.Status);
        Assert.Null(link.ErrorMessage);
    }

    [Fact]
    public void SchemaLink_FailedState_HasErrorMessage()
    {
        var link = new SchemaLink
        {
            Subject = "users-value",
            SourceCluster = "us-east",
            TargetCluster = "eu-west",
            Status = SchemaSyncStatus.Failed,
            ErrorMessage = "Connection refused",
            LastSyncedAt = DateTimeOffset.UtcNow
        };

        Assert.Equal(SchemaSyncStatus.Failed, link.Status);
        Assert.Equal("Connection refused", link.ErrorMessage);
    }

    [Fact]
    public void SubjectPatternMatching_Glob()
    {
        Assert.True(SubjectPatternMatcher.Matches("orders-value", "orders-*"));
        Assert.True(SubjectPatternMatcher.Matches("orders-key", "orders-*"));
        Assert.False(SubjectPatternMatcher.Matches("users-value", "orders-*"));

        Assert.True(SubjectPatternMatcher.Matches("orders-value", "*-value"));
        Assert.True(SubjectPatternMatcher.Matches("users-value", "*-value"));
        Assert.False(SubjectPatternMatcher.Matches("orders-key", "*-value"));

        Assert.True(SubjectPatternMatcher.Matches("abc", "a?c"));
        Assert.False(SubjectPatternMatcher.Matches("abbc", "a?c"));
    }

    [Fact]
    public void SubjectPatternMatching_All()
    {
        Assert.True(SubjectPatternMatcher.Matches("orders-value", "*"));
        Assert.True(SubjectPatternMatcher.Matches("anything", "*"));
        Assert.True(SubjectPatternMatcher.Matches("", "*"));
    }

    [Fact]
    public void SubjectPatternMatching_Exact()
    {
        Assert.True(SubjectPatternMatcher.Matches("orders-value", "orders-value"));
        Assert.False(SubjectPatternMatcher.Matches("orders-key", "orders-value"));
    }

    [Fact]
    public void SubjectPatternMatching_MatchesAny()
    {
        var patterns = new List<string> { "orders-*", "users-*" };

        Assert.True(SubjectPatternMatcher.MatchesAny("orders-value", patterns));
        Assert.True(SubjectPatternMatcher.MatchesAny("users-key", patterns));
        Assert.False(SubjectPatternMatcher.MatchesAny("payments-value", patterns));
    }

    [Fact]
    public void SchemaLinkingState_SaveAndLoad()
    {
        var statePath = Path.Combine(_tempDir, "linking-state.json");

        var state = new SchemaLinkingState();
        state.SetLink("us-east", "orders-value", new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "us-east",
            TargetCluster = "local",
            SourceVersion = 3,
            TargetVersion = 3,
            Status = SchemaSyncStatus.Synced,
            LastSyncedAt = DateTimeOffset.UtcNow
        });
        state.SetLink("eu-west", "users-value", new SchemaLink
        {
            Subject = "users-value",
            SourceCluster = "local",
            TargetCluster = "eu-west",
            SourceVersion = 2,
            TargetVersion = 2,
            Status = SchemaSyncStatus.Synced,
            LastSyncedAt = DateTimeOffset.UtcNow
        });

        state.SaveToFile(statePath);

        Assert.True(File.Exists(statePath));

        var loaded = SchemaLinkingState.LoadFromFile(statePath);

        Assert.Equal(2, loaded.Links.Count);
        Assert.NotNull(loaded.GetLink("us-east", "orders-value"));
        Assert.Equal(3, loaded.GetLink("us-east", "orders-value")!.SourceVersion);
        Assert.NotNull(loaded.GetLink("eu-west", "users-value"));
        Assert.Equal(2, loaded.GetLink("eu-west", "users-value")!.SourceVersion);

        _output.WriteLine($"Loaded {loaded.GetAllLinks().Count} links from file");
    }

    [Fact]
    public void SchemaLinkingState_LoadFromNonExistentFile_ReturnsEmpty()
    {
        var state = SchemaLinkingState.LoadFromFile(Path.Combine(_tempDir, "nonexistent.json"));

        Assert.Empty(state.Links);
        Assert.Empty(state.GetAllLinks());
    }

    [Fact]
    public void SchemaLinkingState_GetAllLinks()
    {
        var state = new SchemaLinkingState();
        state.SetLink("cluster-a", "topic1", new SchemaLink
        {
            Subject = "topic1",
            SourceCluster = "cluster-a",
            TargetCluster = "local",
            SourceVersion = 1,
            TargetVersion = 1,
            LastSyncedAt = DateTimeOffset.UtcNow
        });
        state.SetLink("cluster-b", "topic2", new SchemaLink
        {
            Subject = "topic2",
            SourceCluster = "cluster-b",
            TargetCluster = "local",
            SourceVersion = 2,
            TargetVersion = 2,
            LastSyncedAt = DateTimeOffset.UtcNow
        });

        var allLinks = state.GetAllLinks();
        Assert.Equal(2, allLinks.Count);
    }

    [Fact]
    public void SchemaLinkingState_GetLinksForSubject()
    {
        var state = new SchemaLinkingState();
        state.SetLink("cluster-a", "orders-value", new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "cluster-a",
            TargetCluster = "local",
            SourceVersion = 1,
            TargetVersion = 1,
            LastSyncedAt = DateTimeOffset.UtcNow
        });
        state.SetLink("cluster-b", "orders-value", new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "cluster-b",
            TargetCluster = "local",
            SourceVersion = 2,
            TargetVersion = 2,
            LastSyncedAt = DateTimeOffset.UtcNow
        });
        state.SetLink("cluster-a", "users-value", new SchemaLink
        {
            Subject = "users-value",
            SourceCluster = "cluster-a",
            TargetCluster = "local",
            SourceVersion = 1,
            TargetVersion = 1,
            LastSyncedAt = DateTimeOffset.UtcNow
        });

        var ordersLinks = state.GetLinksForSubject("orders-value");
        Assert.Equal(2, ordersLinks.Count);

        var usersLinks = state.GetLinksForSubject("users-value");
        Assert.Single(usersLinks);
    }

    [Fact]
    public void SchemaLinkingState_GetConflicts()
    {
        var state = new SchemaLinkingState();
        state.SetLink("cluster-a", "orders-value", new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "cluster-a",
            TargetCluster = "local",
            SourceVersion = 3,
            TargetVersion = 2,
            Status = SchemaSyncStatus.Conflict,
            ErrorMessage = "Version conflict: local v2 vs remote v3",
            LastSyncedAt = DateTimeOffset.UtcNow
        });
        state.SetLink("cluster-a", "users-value", new SchemaLink
        {
            Subject = "users-value",
            SourceCluster = "cluster-a",
            TargetCluster = "local",
            SourceVersion = 1,
            TargetVersion = 1,
            Status = SchemaSyncStatus.Synced,
            LastSyncedAt = DateTimeOffset.UtcNow
        });

        var conflicts = state.GetConflicts();
        Assert.Single(conflicts);
        Assert.Equal("orders-value", conflicts[0].Subject);
    }

    [Fact]
    public void SchemaLinkingMetrics_TracksSyncs()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordSync("us-east");
        metrics.RecordSync("us-east");
        metrics.RecordSync("eu-west");

        Assert.Equal(3, metrics.SchemasSynced);
        Assert.Equal(2, metrics.PerClusterSyncCount["us-east"]);
        Assert.Equal(1, metrics.PerClusterSyncCount["eu-west"]);
    }

    [Fact]
    public void SchemaLinkingMetrics_TracksConflicts()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordConflict();
        metrics.RecordConflict();
        metrics.RecordConflictResolved();

        Assert.Equal(2, metrics.ConflictsDetected);
        Assert.Equal(1, metrics.ConflictsResolved);
    }

    [Fact]
    public void SchemaLinkingMetrics_TracksErrors()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordError();
        metrics.RecordError();

        Assert.Equal(2, metrics.SyncErrors);
    }

    [Fact]
    public void SchemaLinkingMetrics_RecordsSyncCycle()
    {
        var metrics = new SchemaLinkingMetrics();

        Assert.Null(metrics.LastSyncAt);

        metrics.RecordSyncCycleComplete();

        Assert.NotNull(metrics.LastSyncAt);
        Assert.True(metrics.LastSyncAt.Value <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void LinkedSchemaRegistry_Properties()
    {
        var remote = new LinkedSchemaRegistry
        {
            ClusterId = "us-east-1",
            SchemaRegistryUrl = "http://schema-registry-east:8081",
            DisplayName = "US East Production"
        };

        Assert.Equal("us-east-1", remote.ClusterId);
        Assert.Equal("http://schema-registry-east:8081", remote.SchemaRegistryUrl);
        Assert.Equal("US East Production", remote.DisplayName);
    }

    [Fact]
    public void LinkedSchemaRegistry_DisplayName_Optional()
    {
        var remote = new LinkedSchemaRegistry
        {
            ClusterId = "eu-west",
            SchemaRegistryUrl = "http://schema-registry-west:8081"
        };

        Assert.Null(remote.DisplayName);
    }

    [Fact]
    public void SchemaSyncStatus_AllValues()
    {
        var values = Enum.GetValues<SchemaSyncStatus>();

        Assert.Equal(4, values.Length);
        Assert.Contains(SchemaSyncStatus.Synced, values);
        Assert.Contains(SchemaSyncStatus.Pending, values);
        Assert.Contains(SchemaSyncStatus.Conflict, values);
        Assert.Contains(SchemaSyncStatus.Failed, values);
    }
}
