namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins Offset ordering/arithmetic operators and the special-value name
/// formatting (Beginning/End/Stored/Unset) used in diagnostics output.
/// </summary>
public class OffsetOperatorTests
{
    [Theory]
    [InlineData(-2, "Beginning")]
    [InlineData(-1, "End")]
    [InlineData(-1000, "Stored")]
    [InlineData(-1001, "Unset")]
    public void ToString_SpecialValues_UseSymbolicNames(long value, string expected)
    {
        Assert.Equal(expected, new Offset(value).ToString());
    }

    [Fact]
    public void ToString_UnknownNegativeValue_IsBracketed()
    {
        Assert.Equal("[-5]", new Offset(-5).ToString());
    }

    [Fact]
    public void ComparisonOperators_OrderByValue()
    {
        var low = new Offset(10);
        var high = new Offset(20);

        Assert.True(low < high);
        Assert.True(high > low);
        Assert.True(low <= high);
        Assert.True(high >= low);
        Assert.True(low <= new Offset(10));
        Assert.True(low >= new Offset(10));
        Assert.False(high < low);
    }

    [Fact]
    public void CompareTo_ReturnsOrdering()
    {
        Assert.True(new Offset(1).CompareTo(new Offset(2)) < 0);
        Assert.True(new Offset(2).CompareTo(new Offset(1)) > 0);
        Assert.Equal(0, new Offset(5).CompareTo(new Offset(5)));
    }

    [Fact]
    public void SpecialOffsets_OrderBeginningBeforeEnd()
    {
        Assert.True(Offset.Beginning < Offset.End);
    }

    [Fact]
    public void AdditionOperator_AddsValue()
    {
        var offset = new Offset(10) + 5;
        Assert.Equal(15, offset.Value);
    }

    [Fact]
    public void SubtractionOperator_SubtractsValue()
    {
        var offset = new Offset(10) - 3;
        Assert.Equal(7, offset.Value);
    }
}
