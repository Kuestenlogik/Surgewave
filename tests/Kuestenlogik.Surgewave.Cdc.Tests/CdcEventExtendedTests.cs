using System.Text.Json;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Extended tests for CdcEvent serialization, deserialization, and edge cases.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcEventExtendedTests
{
    [Fact]
    public void CdcEvent_Deserialization_PreservesAllFields()
    {
        var json = """
        {
            "Operation": 0,
            "Schema": "public",
            "Table": "orders",
            "After": {"id": 1, "name": "Widget"},
            "Timestamp": "2026-03-15T12:00:00+00:00",
            "Lsn": 42
        }
        """;

        var evt = JsonSerializer.Deserialize<CdcEvent>(json);

        Assert.NotNull(evt);
        Assert.Equal(CdcOperation.Insert, evt.Operation);
        Assert.Equal("public", evt.Schema);
        Assert.Equal("orders", evt.Table);
        Assert.NotNull(evt.After);
        Assert.Null(evt.Before);
        Assert.Equal(42, evt.Lsn);
    }

    [Fact]
    public void CdcEvent_WithNullValues_InDictionary_Roundtrips()
    {
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Update,
            Schema = "public",
            Table = "users",
            Before = new Dictionary<string, object?> { ["email"] = "old@test.com", ["phone"] = null },
            After = new Dictionary<string, object?> { ["email"] = "new@test.com", ["phone"] = null },
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 500
        };

        var json = JsonSerializer.Serialize(evt);
        var deserialized = JsonSerializer.Deserialize<CdcEvent>(json);

        Assert.NotNull(deserialized);
        Assert.NotNull(deserialized.Before);
        Assert.NotNull(deserialized.After);
        Assert.Equal(CdcOperation.Update, deserialized.Operation);
    }

    [Fact]
    public void CdcEvent_EmptyDictionaries_Roundtrip()
    {
        var evt = new CdcEvent
        {
            Operation = CdcOperation.Insert,
            Schema = "public",
            Table = "empty_table",
            After = new Dictionary<string, object?>(),
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 1
        };

        var json = JsonSerializer.Serialize(evt);
        var deserialized = JsonSerializer.Deserialize<CdcEvent>(json);

        Assert.NotNull(deserialized);
        Assert.NotNull(deserialized.After);
        Assert.Empty(deserialized.After);
    }

    [Fact]
    public void CdcEvent_RecordEquality_SameValues_AreEqual()
    {
        var ts = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var evt1 = new CdcEvent
        {
            Operation = CdcOperation.Delete,
            Schema = "public",
            Table = "orders",
            Timestamp = ts,
            Lsn = 100
        };
        var evt2 = new CdcEvent
        {
            Operation = CdcOperation.Delete,
            Schema = "public",
            Table = "orders",
            Timestamp = ts,
            Lsn = 100
        };

        Assert.Equal(evt1, evt2);
    }

    [Fact]
    public void CdcEvent_RecordEquality_DifferentLsn_AreNotEqual()
    {
        var ts = DateTimeOffset.UtcNow;
        var evt1 = new CdcEvent
        {
            Operation = CdcOperation.Insert,
            Schema = "public",
            Table = "orders",
            Timestamp = ts,
            Lsn = 100
        };
        var evt2 = new CdcEvent
        {
            Operation = CdcOperation.Insert,
            Schema = "public",
            Table = "orders",
            Timestamp = ts,
            Lsn = 200
        };

        Assert.NotEqual(evt1, evt2);
    }

    [Fact]
    public void CdcEvent_LargeAfterDictionary_Serializes()
    {
        var after = new Dictionary<string, object?>();
        for (var i = 0; i < 100; i++)
        {
            after[$"column_{i}"] = $"value_{i}";
        }

        var evt = new CdcEvent
        {
            Operation = CdcOperation.Insert,
            Schema = "data",
            Table = "wide_table",
            After = after,
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = 999
        };

        var json = JsonSerializer.Serialize(evt);
        Assert.NotNull(json);
        Assert.Contains("column_0", json);
        Assert.Contains("column_99", json);
    }
}
