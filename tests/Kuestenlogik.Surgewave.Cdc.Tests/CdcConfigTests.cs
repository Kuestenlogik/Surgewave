using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for CdcConfig default values and validation.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcConfigTests
{
    [Fact]
    public void CdcConfig_Defaults_AreCorrect()
    {
        // Act
        var config = new CdcConfig();

        // Assert
        Assert.Equal("", config.ConnectionString);
        Assert.Equal("surgewave_cdc", config.SlotName);
        Assert.Equal("surgewave_publication", config.PublicationName);
        Assert.Empty(config.Tables);
        Assert.Equal("cdc.", config.TopicPrefix);
        Assert.True(config.IncludeSchema);
        Assert.False(config.SnapshotOnStart);
        Assert.Equal(10, config.AckIntervalSeconds);
        Assert.False(config.Enabled);
    }

    [Fact]
    public void CdcConfig_SectionName_IsCorrect()
    {
        Assert.Equal("Surgewave:Cdc", CdcConfig.SectionName);
    }

    [Fact]
    public void CdcConfig_CustomValues_ArePreserved()
    {
        // Arrange & Act
        var config = new CdcConfig
        {
            ConnectionString = "Host=localhost;Database=mydb;Username=user;Password=pass",
            SlotName = "my_slot",
            PublicationName = "my_pub",
            Tables = ["public.orders", "public.products"],
            TopicPrefix = "db.",
            IncludeSchema = false,
            SnapshotOnStart = true,
            AckIntervalSeconds = 5,
            Enabled = true
        };

        // Assert
        Assert.Equal("Host=localhost;Database=mydb;Username=user;Password=pass", config.ConnectionString);
        Assert.Equal("my_slot", config.SlotName);
        Assert.Equal("my_pub", config.PublicationName);
        Assert.Equal(2, config.Tables.Count);
        Assert.Equal("db.", config.TopicPrefix);
        Assert.False(config.IncludeSchema);
        Assert.True(config.SnapshotOnStart);
        Assert.Equal(5, config.AckIntervalSeconds);
        Assert.True(config.Enabled);
    }
}
