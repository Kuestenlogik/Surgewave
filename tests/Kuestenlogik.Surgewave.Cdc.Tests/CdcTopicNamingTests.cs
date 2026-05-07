using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for CDC topic naming conventions.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcTopicNamingTests
{
    [Fact]
    public void GetTopicName_WithSchema_IncludesSchemaInName()
    {
        // Arrange
        var config = new CdcConfig { TopicPrefix = "cdc.", IncludeSchema = true };

        // Act
        var topic = CdcTopicNaming.GetTopicName(config, "public", "orders");

        // Assert
        Assert.Equal("cdc.public.orders", topic);
    }

    [Fact]
    public void GetTopicName_WithoutSchema_OmitsSchemaFromName()
    {
        // Arrange
        var config = new CdcConfig { TopicPrefix = "cdc.", IncludeSchema = false };

        // Act
        var topic = CdcTopicNaming.GetTopicName(config, "public", "orders");

        // Assert
        Assert.Equal("cdc.orders", topic);
    }

    [Fact]
    public void GetTopicName_EmptyPrefix_NoPrefix()
    {
        // Arrange
        var config = new CdcConfig { TopicPrefix = "", IncludeSchema = true };

        // Act
        var topic = CdcTopicNaming.GetTopicName(config, "myschema", "users");

        // Assert
        Assert.Equal("myschema.users", topic);
    }

    [Fact]
    public void GetTopicName_CustomPrefix_AppliesPrefix()
    {
        // Arrange
        var config = new CdcConfig { TopicPrefix = "db.changes.", IncludeSchema = true };

        // Act
        var topic = CdcTopicNaming.GetTopicName(config, "public", "products");

        // Assert
        Assert.Equal("db.changes.public.products", topic);
    }

    [Fact]
    public void GetMessageKey_SingleColumn_ReturnsValue()
    {
        // Arrange
        var row = new Dictionary<string, object?> { ["id"] = 42L, ["name"] = "test" };
        var pkColumns = new List<string> { "id" };

        // Act
        var key = CdcTopicNaming.GetMessageKey(row, pkColumns);

        // Assert
        Assert.Equal("42", key);
    }

    [Fact]
    public void GetMessageKey_MultipleColumns_ReturnsJson()
    {
        // Arrange
        var row = new Dictionary<string, object?>
        {
            ["region"] = "us-east",
            ["order_id"] = 100L,
            ["name"] = "test"
        };
        var pkColumns = new List<string> { "region", "order_id" };

        // Act
        var key = CdcTopicNaming.GetMessageKey(row, pkColumns);

        // Assert
        Assert.NotNull(key);
        Assert.Contains("region", key);
        Assert.Contains("us-east", key);
        Assert.Contains("order_id", key);
    }

    [Fact]
    public void GetMessageKey_NullRow_ReturnsNull()
    {
        // Act
        var key = CdcTopicNaming.GetMessageKey(null, ["id"]);

        // Assert
        Assert.Null(key);
    }

    [Fact]
    public void GetMessageKey_NoPrimaryKeyColumns_ReturnsNull()
    {
        // Arrange
        var row = new Dictionary<string, object?> { ["id"] = 1L };

        // Act
        var key = CdcTopicNaming.GetMessageKey(row, null);

        // Assert
        Assert.Null(key);
    }

    [Fact]
    public void GetMessageKey_EmptyPrimaryKeyColumns_ReturnsNull()
    {
        // Arrange
        var row = new Dictionary<string, object?> { ["id"] = 1L };

        // Act
        var key = CdcTopicNaming.GetMessageKey(row, []);

        // Assert
        Assert.Null(key);
    }

    [Fact]
    public void GetTopicName_NullConfig_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            CdcTopicNaming.GetTopicName(null!, "public", "orders"));
    }

    [Fact]
    public void GetTopicName_EmptyTable_Throws()
    {
        var config = new CdcConfig();
        Assert.Throws<ArgumentException>(() =>
            CdcTopicNaming.GetTopicName(config, "public", ""));
    }
}
