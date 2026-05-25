using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for the CdcOperation enum values.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcOperationTests
{
    [Fact]
    public void AllValues_Exist()
    {
        var values = Enum.GetValues<CdcOperation>();

        Assert.Contains(CdcOperation.Insert, values);
        Assert.Contains(CdcOperation.Update, values);
        Assert.Contains(CdcOperation.Delete, values);
        Assert.Contains(CdcOperation.Snapshot, values);
        Assert.Equal(4, values.Length);
    }

    [Theory]
    [InlineData(CdcOperation.Insert, "Insert")]
    [InlineData(CdcOperation.Update, "Update")]
    [InlineData(CdcOperation.Delete, "Delete")]
    [InlineData(CdcOperation.Snapshot, "Snapshot")]
    public void ToString_ReturnsExpected(CdcOperation operation, string expected)
    {
        Assert.Equal(expected, operation.ToString());
    }

    [Theory]
    [InlineData(0, CdcOperation.Insert)]
    [InlineData(1, CdcOperation.Update)]
    [InlineData(2, CdcOperation.Delete)]
    [InlineData(3, CdcOperation.Snapshot)]
    public void IntegerValues_MatchExpected(int intValue, CdcOperation expected)
    {
        Assert.Equal(expected, (CdcOperation)intValue);
    }

    [Fact]
    public void IsDefined_ValidValues_ReturnsTrue()
    {
        Assert.True(Enum.IsDefined(CdcOperation.Insert));
        Assert.True(Enum.IsDefined(CdcOperation.Update));
        Assert.True(Enum.IsDefined(CdcOperation.Delete));
        Assert.True(Enum.IsDefined(CdcOperation.Snapshot));
    }

    [Fact]
    public void IsDefined_InvalidValue_ReturnsFalse()
    {
        Assert.False(Enum.IsDefined((CdcOperation)99));
    }
}
