using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for storage policy configurations (RetentionPolicy, CleanupPolicy, CompactionConfig).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class StoragePolicyTests
{
    #region RetentionPolicy Tests

    [Fact]
    public void RetentionPolicy_DefaultValues()
    {
        // Act
        var policy = new RetentionPolicy();

        // Assert
        Assert.Equal(168, policy.RetentionHours); // 7 days
        Assert.Equal(-1, policy.RetentionBytes);  // unlimited
        Assert.Equal(1, policy.MinSegmentsToKeep);
    }

    [Fact]
    public void RetentionPolicy_Default_StaticProperty()
    {
        // Act
        var policy = RetentionPolicy.Default;

        // Assert
        Assert.Equal(168, policy.RetentionHours);
        Assert.Equal(-1, policy.RetentionBytes);
        Assert.Equal(1, policy.MinSegmentsToKeep);
    }

    [Fact]
    public void RetentionPolicy_Unlimited_StaticProperty()
    {
        // Act
        var policy = RetentionPolicy.Unlimited;

        // Assert
        Assert.Equal(-1, policy.RetentionHours);
        Assert.Equal(-1, policy.RetentionBytes);
    }

    [Fact]
    public void RetentionPolicy_CustomValues()
    {
        // Act
        var policy = new RetentionPolicy
        {
            RetentionHours = 24,
            RetentionBytes = 1073741824, // 1GB
            MinSegmentsToKeep = 3
        };

        // Assert
        Assert.Equal(24, policy.RetentionHours);
        Assert.Equal(1073741824, policy.RetentionBytes);
        Assert.Equal(3, policy.MinSegmentsToKeep);
    }

    [Fact]
    public void RetentionPolicy_WithExpression_CreatesModifiedCopy()
    {
        // Arrange
        var original = new RetentionPolicy { RetentionHours = 168 };

        // Act
        var modified = original with { RetentionHours = 24 };

        // Assert
        Assert.Equal(168, original.RetentionHours);
        Assert.Equal(24, modified.RetentionHours);
    }

    [Fact]
    public void RetentionPolicy_Equality_SameValues_AreEqual()
    {
        // Arrange
        var policy1 = new RetentionPolicy { RetentionHours = 24 };
        var policy2 = new RetentionPolicy { RetentionHours = 24 };

        // Assert
        Assert.Equal(policy1, policy2);
    }

    [Fact]
    public void RetentionPolicy_Equality_DifferentValues_NotEqual()
    {
        // Arrange
        var policy1 = new RetentionPolicy { RetentionHours = 24 };
        var policy2 = new RetentionPolicy { RetentionHours = 48 };

        // Assert
        Assert.NotEqual(policy1, policy2);
    }

    [Fact]
    public void RetentionPolicy_ShortRetention_OneDayInHours()
    {
        // Act
        var policy = new RetentionPolicy { RetentionHours = 24 };

        // Assert
        Assert.Equal(24, policy.RetentionHours);
    }

    [Fact]
    public void RetentionPolicy_LongRetention_ThirtyDays()
    {
        // Act
        var policy = new RetentionPolicy { RetentionHours = 30 * 24 };

        // Assert
        Assert.Equal(720, policy.RetentionHours);
    }

    [Fact]
    public void RetentionPolicy_SizeBasedRetention()
    {
        // Act - 10GB retention limit
        var policy = new RetentionPolicy
        {
            RetentionHours = -1, // unlimited time
            RetentionBytes = 10L * 1024 * 1024 * 1024
        };

        // Assert
        Assert.Equal(-1, policy.RetentionHours);
        Assert.Equal(10L * 1024 * 1024 * 1024, policy.RetentionBytes);
    }

    #endregion

    #region CleanupPolicy Tests

    [Fact]
    public void CleanupPolicy_Delete_HasCorrectValue()
    {
        // Assert
        Assert.Equal(1, (int)CleanupPolicy.Delete);
    }

    [Fact]
    public void CleanupPolicy_Compact_HasCorrectValue()
    {
        // Assert
        Assert.Equal(2, (int)CleanupPolicy.Compact);
    }

    [Fact]
    public void CleanupPolicy_DeleteAndCompact_IsCombination()
    {
        // Assert
        Assert.Equal(CleanupPolicy.Delete | CleanupPolicy.Compact, CleanupPolicy.DeleteAndCompact);
        Assert.Equal(3, (int)CleanupPolicy.DeleteAndCompact);
    }

    [Fact]
    public void CleanupPolicy_HasDelete_TrueForDelete()
    {
        // Arrange
        var policy = CleanupPolicy.Delete;

        // Assert
        Assert.True(policy.HasFlag(CleanupPolicy.Delete));
        Assert.False(policy.HasFlag(CleanupPolicy.Compact));
    }

    [Fact]
    public void CleanupPolicy_HasCompact_TrueForCompact()
    {
        // Arrange
        var policy = CleanupPolicy.Compact;

        // Assert
        Assert.False(policy.HasFlag(CleanupPolicy.Delete));
        Assert.True(policy.HasFlag(CleanupPolicy.Compact));
    }

    [Fact]
    public void CleanupPolicy_DeleteAndCompact_HasBothFlags()
    {
        // Arrange
        var policy = CleanupPolicy.DeleteAndCompact;

        // Assert
        Assert.True(policy.HasFlag(CleanupPolicy.Delete));
        Assert.True(policy.HasFlag(CleanupPolicy.Compact));
    }

    [Fact]
    public void CleanupPolicy_CanCombineWithBitwiseOr()
    {
        // Act
        var policy = CleanupPolicy.Delete | CleanupPolicy.Compact;

        // Assert
        Assert.Equal(CleanupPolicy.DeleteAndCompact, policy);
    }

    [Fact]
    public void CleanupPolicy_EnumIsDefined()
    {
        // Assert
        Assert.True(Enum.IsDefined(CleanupPolicy.Delete));
        Assert.True(Enum.IsDefined(CleanupPolicy.Compact));
        Assert.True(Enum.IsDefined(CleanupPolicy.DeleteAndCompact));
    }

    #endregion

    #region CompactionConfig Tests

    [Fact]
    public void CompactionConfig_DefaultValues()
    {
        // Act
        var config = new CompactionConfig();

        // Assert
        Assert.Equal(0, config.MinCompactionLagMs);
        Assert.Equal(24 * 60 * 60 * 1000, config.DeleteRetentionMs); // 24 hours
        Assert.Equal(0.5, config.MinCleanableDirtyRatio);
        Assert.Equal(0, config.MaxCompactionBytes);
    }

    [Fact]
    public void CompactionConfig_Default_StaticProperty()
    {
        // Act
        var config = CompactionConfig.Default;

        // Assert
        Assert.Equal(0, config.MinCompactionLagMs);
        Assert.Equal(24 * 60 * 60 * 1000, config.DeleteRetentionMs);
        Assert.Equal(0.5, config.MinCleanableDirtyRatio);
        Assert.Equal(0, config.MaxCompactionBytes);
    }

    [Fact]
    public void CompactionConfig_CustomValues()
    {
        // Act
        var config = new CompactionConfig
        {
            MinCompactionLagMs = 60000,
            DeleteRetentionMs = 48 * 60 * 60 * 1000,
            MinCleanableDirtyRatio = 0.3,
            MaxCompactionBytes = 1073741824
        };

        // Assert
        Assert.Equal(60000, config.MinCompactionLagMs);
        Assert.Equal(48 * 60 * 60 * 1000, config.DeleteRetentionMs);
        Assert.Equal(0.3, config.MinCleanableDirtyRatio);
        Assert.Equal(1073741824, config.MaxCompactionBytes);
    }

    [Fact]
    public void CompactionConfig_WithExpression_CreatesModifiedCopy()
    {
        // Arrange
        var original = new CompactionConfig { MinCleanableDirtyRatio = 0.5 };

        // Act
        var modified = original with { MinCleanableDirtyRatio = 0.8 };

        // Assert
        Assert.Equal(0.5, original.MinCleanableDirtyRatio);
        Assert.Equal(0.8, modified.MinCleanableDirtyRatio);
    }

    [Fact]
    public void CompactionConfig_Equality_SameValues_AreEqual()
    {
        // Arrange
        var config1 = new CompactionConfig { MinCompactionLagMs = 1000 };
        var config2 = new CompactionConfig { MinCompactionLagMs = 1000 };

        // Assert
        Assert.Equal(config1, config2);
    }

    [Fact]
    public void CompactionConfig_Equality_DifferentValues_NotEqual()
    {
        // Arrange
        var config1 = new CompactionConfig { MinCompactionLagMs = 1000 };
        var config2 = new CompactionConfig { MinCompactionLagMs = 2000 };

        // Assert
        Assert.NotEqual(config1, config2);
    }

    [Fact]
    public void CompactionConfig_DeleteRetentionMs_DefaultIs24Hours()
    {
        // Arrange
        var config = new CompactionConfig();

        // Assert - 24 hours in milliseconds
        Assert.Equal(86400000L, config.DeleteRetentionMs);
    }

    [Fact]
    public void CompactionConfig_MinCleanableDirtyRatio_RangeValues()
    {
        // Act - Test valid ratio values
        var lowRatio = new CompactionConfig { MinCleanableDirtyRatio = 0.1 };
        var highRatio = new CompactionConfig { MinCleanableDirtyRatio = 0.9 };

        // Assert
        Assert.Equal(0.1, lowRatio.MinCleanableDirtyRatio);
        Assert.Equal(0.9, highRatio.MinCleanableDirtyRatio);
    }

    [Fact]
    public void CompactionConfig_AggressiveCompaction_LowDirtyRatio()
    {
        // Act - aggressive compaction with low dirty ratio
        var config = new CompactionConfig
        {
            MinCleanableDirtyRatio = 0.1, // Compact at 10% dirty
            MinCompactionLagMs = 0        // No delay
        };

        // Assert
        Assert.Equal(0.1, config.MinCleanableDirtyRatio);
        Assert.Equal(0, config.MinCompactionLagMs);
    }

    [Fact]
    public void CompactionConfig_DelayedCompaction_WithLag()
    {
        // Act - delayed compaction with lag
        var config = new CompactionConfig
        {
            MinCompactionLagMs = 3600000 // 1 hour delay before eligible for compaction
        };

        // Assert
        Assert.Equal(3600000, config.MinCompactionLagMs);
    }

    [Fact]
    public void CompactionConfig_LimitedCompactionBytes()
    {
        // Act - limit bytes per compaction run
        var config = new CompactionConfig
        {
            MaxCompactionBytes = 500 * 1024 * 1024 // 500MB per run
        };

        // Assert
        Assert.Equal(500L * 1024 * 1024, config.MaxCompactionBytes);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void RetentionAndCompaction_TypicalConfiguration()
    {
        // Act - typical production configuration
        var retention = new RetentionPolicy
        {
            RetentionHours = 7 * 24, // 7 days
            RetentionBytes = 50L * 1024 * 1024 * 1024, // 50GB
            MinSegmentsToKeep = 2
        };

        var compaction = new CompactionConfig
        {
            MinCompactionLagMs = 0,
            DeleteRetentionMs = 24 * 60 * 60 * 1000,
            MinCleanableDirtyRatio = 0.5
        };

        // Assert
        Assert.Equal(168, retention.RetentionHours);
        Assert.Equal(50L * 1024 * 1024 * 1024, retention.RetentionBytes);
        Assert.Equal(0.5, compaction.MinCleanableDirtyRatio);
    }

    [Fact]
    public void CompactedTopic_Configuration()
    {
        // Typical compacted topic config (like Kafka __consumer_offsets)
        var retention = new RetentionPolicy
        {
            RetentionHours = -1, // Never delete based on time
            RetentionBytes = -1  // Never delete based on size
        };

        var cleanup = CleanupPolicy.Compact;

        var compaction = new CompactionConfig
        {
            DeleteRetentionMs = 7 * 24 * 60 * 60 * 1000, // Keep tombstones for 7 days
            MinCleanableDirtyRatio = 0.5
        };

        // Assert
        Assert.Equal(-1, retention.RetentionHours);
        Assert.Equal(-1, retention.RetentionBytes);
        Assert.Equal(CleanupPolicy.Compact, cleanup);
        Assert.Equal(7L * 24 * 60 * 60 * 1000, compaction.DeleteRetentionMs);
    }

    #endregion
}
