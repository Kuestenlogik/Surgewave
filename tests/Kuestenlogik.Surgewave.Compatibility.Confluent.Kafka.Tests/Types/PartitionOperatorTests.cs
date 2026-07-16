namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins Partition ordering operators, the IsSpecial flag, and the bracketed
/// formatting of special (negative) partition values such as Partition.Any.
/// </summary>
public class PartitionOperatorTests
{
    [Fact]
    public void Any_IsSpecialAndFormatsBracketed()
    {
        Assert.True(Partition.Any.IsSpecial);
        Assert.Equal("[-1]", Partition.Any.ToString());
    }

    [Fact]
    public void RegularPartition_IsNotSpecial()
    {
        Assert.False(new Partition(0).IsSpecial);
    }

    [Fact]
    public void ComparisonOperators_OrderByValue()
    {
        var low = new Partition(1);
        var high = new Partition(2);

        Assert.True(low < high);
        Assert.True(high > low);
        Assert.True(low <= high);
        Assert.True(high >= low);
        Assert.True(low <= new Partition(1));
        Assert.True(low >= new Partition(1));
        Assert.False(high < low);
    }

    [Fact]
    public void CompareTo_ReturnsOrdering()
    {
        Assert.True(new Partition(1).CompareTo(new Partition(2)) < 0);
        Assert.True(new Partition(2).CompareTo(new Partition(1)) > 0);
        Assert.Equal(0, new Partition(3).CompareTo(new Partition(3)));
    }
}
