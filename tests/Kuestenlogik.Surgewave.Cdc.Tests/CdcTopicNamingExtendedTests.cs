using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Extended tests for CdcTopicNaming edge cases and GetMessageKey scenarios.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcTopicNamingExtendedTests
{
    [Fact]
    public void GetTopicName_NullSchema_WithIncludeSchema_OmitsSchema()
    {
        var config = new CdcConfig { TopicPrefix = "cdc.", IncludeSchema = true };

        // When schema is null/whitespace, it should be omitted even with IncludeSchema = true
        var topic = CdcTopicNaming.GetTopicName(config, "", "orders");

        Assert.Equal("cdc.orders", topic);
    }

    [Fact]
    public void GetTopicName_WhitespaceSchema_WithIncludeSchema_OmitsSchema()
    {
        var config = new CdcConfig { TopicPrefix = "cdc.", IncludeSchema = true };

        var topic = CdcTopicNaming.GetTopicName(config, "   ", "orders");

        Assert.Equal("cdc.orders", topic);
    }

    [Fact]
    public void GetTopicName_NullPrefix_TreatedAsEmpty()
    {
        var config = new CdcConfig { TopicPrefix = null!, IncludeSchema = true };

        var topic = CdcTopicNaming.GetTopicName(config, "public", "users");

        Assert.Equal("public.users", topic);
    }

    [Fact]
    public void GetTopicName_LongTableName_Works()
    {
        var config = new CdcConfig { TopicPrefix = "cdc.", IncludeSchema = true };
        var longTable = new string('x', 200);

        var topic = CdcTopicNaming.GetTopicName(config, "schema", longTable);

        Assert.Equal($"cdc.schema.{longTable}", topic);
    }

    [Fact]
    public void GetMessageKey_SingleColumn_MissingFromRow_ReturnsNull()
    {
        var row = new Dictionary<string, object?> { ["name"] = "test" };
        var pkColumns = new List<string> { "id" };

        var key = CdcTopicNaming.GetMessageKey(row, pkColumns);

        Assert.Null(key);
    }

    [Fact]
    public void GetMessageKey_SingleColumn_NullValue_ReturnsNull()
    {
        var row = new Dictionary<string, object?> { ["id"] = null };
        var pkColumns = new List<string> { "id" };

        var key = CdcTopicNaming.GetMessageKey(row, pkColumns);

        Assert.Null(key);
    }

    [Fact]
    public void GetMessageKey_MultiColumn_PartiallyMissing_IncludesAvailable()
    {
        var row = new Dictionary<string, object?>
        {
            ["region"] = "us-east",
            ["extra"] = "data"
        };
        var pkColumns = new List<string> { "region", "order_id" };

        var key = CdcTopicNaming.GetMessageKey(row, pkColumns);

        Assert.NotNull(key);
        Assert.Contains("region", key);
        Assert.Contains("us-east", key);
    }
}
