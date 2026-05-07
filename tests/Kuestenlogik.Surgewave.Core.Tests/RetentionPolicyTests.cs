using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for RetentionPolicy, CleanupPolicy, and CompactionConfig.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RetentionPolicyTests
{
    #region RetentionPolicy Tests

    [Fact]
    public void RetentionPolicy_Default_Has7DayRetention()
    {
        var policy = RetentionPolicy.Default;

        Assert.Equal(168, policy.RetentionHours); // 7 days
        Assert.Equal(-1, policy.RetentionBytes);   // unlimited size
        Assert.Equal(1, policy.MinSegmentsToKeep);
    }

    [Fact]
    public void RetentionPolicy_Unlimited_HasNegativeOneValues()
    {
        var policy = RetentionPolicy.Unlimited;

        Assert.Equal(-1, policy.RetentionHours);
        Assert.Equal(-1, policy.RetentionBytes);
    }

    [Fact]
    public void RetentionPolicy_CustomValues_SetCorrectly()
    {
        var policy = new RetentionPolicy
        {
            RetentionHours = 24,
            RetentionBytes = 1024 * 1024 * 1024L, // 1 GB
            MinSegmentsToKeep = 3
        };

        Assert.Equal(24, policy.RetentionHours);
        Assert.Equal(1024 * 1024 * 1024L, policy.RetentionBytes);
        Assert.Equal(3, policy.MinSegmentsToKeep);
    }

    [Fact]
    public void RetentionPolicy_Equality_SameValues_AreEqual()
    {
        var a = new RetentionPolicy { RetentionHours = 48, RetentionBytes = 100 };
        var b = new RetentionPolicy { RetentionHours = 48, RetentionBytes = 100 };

        Assert.Equal(a, b);
    }

    [Fact]
    public void RetentionPolicy_Equality_DifferentValues_NotEqual()
    {
        var a = new RetentionPolicy { RetentionHours = 48 };
        var b = new RetentionPolicy { RetentionHours = 24 };

        Assert.NotEqual(a, b);
    }

    #endregion

    #region CleanupPolicy Tests

    [Fact]
    public void CleanupPolicy_Delete_HasCorrectValue()
    {
        Assert.Equal(1, (int)CleanupPolicy.Delete);
    }

    [Fact]
    public void CleanupPolicy_Compact_HasCorrectValue()
    {
        Assert.Equal(2, (int)CleanupPolicy.Compact);
    }

    [Fact]
    public void CleanupPolicy_DeleteAndCompact_IsCombinationOfFlags()
    {
        Assert.Equal(CleanupPolicy.Delete | CleanupPolicy.Compact, CleanupPolicy.DeleteAndCompact);
        Assert.True(CleanupPolicy.DeleteAndCompact.HasFlag(CleanupPolicy.Delete));
        Assert.True(CleanupPolicy.DeleteAndCompact.HasFlag(CleanupPolicy.Compact));
    }

    [Fact]
    public void CleanupPolicy_Ephemeral_HasCorrectValue()
    {
        Assert.Equal(4, (int)CleanupPolicy.Ephemeral);
        Assert.False(CleanupPolicy.Ephemeral.HasFlag(CleanupPolicy.Delete));
        Assert.False(CleanupPolicy.Ephemeral.HasFlag(CleanupPolicy.Compact));
    }

    #endregion

    #region CompactionConfig Tests

    [Fact]
    public void CompactionConfig_Default_HasCorrectValues()
    {
        var config = CompactionConfig.Default;

        Assert.Equal(0, config.MinCompactionLagMs);
        Assert.Equal(24 * 60 * 60 * 1000L, config.DeleteRetentionMs); // 24 hours
        Assert.Equal(0.5, config.MinCleanableDirtyRatio);
        Assert.Equal(0, config.MaxCompactionBytes);
    }

    [Fact]
    public void CompactionConfig_CustomValues_SetCorrectly()
    {
        var config = new CompactionConfig
        {
            MinCompactionLagMs = 60000,
            DeleteRetentionMs = 3600000,
            MinCleanableDirtyRatio = 0.3,
            MaxCompactionBytes = 1024 * 1024 * 100
        };

        Assert.Equal(60000, config.MinCompactionLagMs);
        Assert.Equal(3600000, config.DeleteRetentionMs);
        Assert.Equal(0.3, config.MinCleanableDirtyRatio);
        Assert.Equal(1024 * 1024 * 100L, config.MaxCompactionBytes);
    }

    #endregion
}
