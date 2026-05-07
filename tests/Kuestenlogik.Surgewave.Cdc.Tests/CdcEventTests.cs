using System.Text.Json;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for CdcEvent serialization and construction.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcEventTests
{
    [Fact]
    public void CdcEvent_Serialization_RoundTrips()
    {
        // Arrange
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Insert,
            Schema = "public",
            Table = "orders",
            After = new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["name"] = "Test Order",
                ["amount"] = 99.99,
                ["created_at"] = "2026-01-15T10:30:00Z"
            },
            Timestamp = new DateTimeOffset(2026, 3, 15, 12, 0, 0, TimeSpan.Zero),
            Lsn = 12345678L
        };

        // Act
        var json = JsonSerializer.Serialize(evt);
        var deserialized = JsonSerializer.Deserialize<CdcEvent>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(CdcOperation.Insert, deserialized.Operation);
        Assert.Equal("public", deserialized.Schema);
        Assert.Equal("orders", deserialized.Table);
        Assert.NotNull(deserialized.After);
        Assert.Null(deserialized.Before);
        Assert.Equal(12345678L, deserialized.Lsn);
    }

    [Fact]
    public void CdcEvent_Update_HasBeforeAndAfter()
    {
        // Arrange & Act
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Update,
            Schema = "public",
            Table = "users",
            Before = new Dictionary<string, object?> { ["name"] = "Old Name" },
            After = new Dictionary<string, object?> { ["name"] = "New Name" },
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 100
        };

        // Assert
        Assert.Equal(CdcOperation.Update, evt.Operation);
        Assert.NotNull(evt.Before);
        Assert.NotNull(evt.After);
        Assert.Equal("Old Name", evt.Before["name"]?.ToString());
        Assert.Equal("New Name", evt.After["name"]?.ToString());
    }

    [Fact]
    public void CdcEvent_Delete_HasBeforeOnly()
    {
        // Arrange & Act
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Delete,
            Schema = "public",
            Table = "products",
            Before = new Dictionary<string, object?> { ["id"] = 42L },
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 200
        };

        // Assert
        Assert.Equal(CdcOperation.Delete, evt.Operation);
        Assert.NotNull(evt.Before);
        Assert.Null(evt.After);
    }

    [Fact]
    public void CdcEvent_Snapshot_Operation()
    {
        // Arrange & Act
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Snapshot,
            Schema = "public",
            Table = "inventory",
            After = new Dictionary<string, object?> { ["sku"] = "ABC-123", ["quantity"] = 50L },
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 0
        };

        // Assert
        Assert.Equal(CdcOperation.Snapshot, evt.Operation);
        Assert.NotNull(evt.After);
        Assert.Equal(2, evt.After.Count);
    }
}
