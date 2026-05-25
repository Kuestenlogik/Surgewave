namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

public class PartitionTests
{
    [Fact]
    public void Constructor_WithValue_SetsValue()
    {
        var partition = new Partition(5);
        Assert.Equal(5, partition.Value);
    }

    [Fact]
    public void Any_HasSpecialValue()
    {
        Assert.Equal(-1, Partition.Any.Value);
    }

    [Fact]
    public void Equality_SameValue_ReturnsTrue()
    {
        var p1 = new Partition(3);
        var p2 = new Partition(3);
        Assert.Equal(p1, p2);
        Assert.True(p1 == p2);
    }

    [Fact]
    public void Equality_DifferentValue_ReturnsFalse()
    {
        var p1 = new Partition(3);
        var p2 = new Partition(5);
        Assert.NotEqual(p1, p2);
        Assert.True(p1 != p2);
    }

    [Fact]
    public void ImplicitConversion_FromInt_Works()
    {
        Partition p = 7;
        Assert.Equal(7, p.Value);
    }

    [Fact]
    public void ImplicitConversion_ToInt_Works()
    {
        var partition = new Partition(9);
        int value = partition;
        Assert.Equal(9, value);
    }

    [Fact]
    public void ToString_ReturnsValue()
    {
        var partition = new Partition(42);
        Assert.Equal("42", partition.ToString());
    }

    [Fact]
    public void GetHashCode_SameValue_SameHash()
    {
        var p1 = new Partition(5);
        var p2 = new Partition(5);
        Assert.Equal(p1.GetHashCode(), p2.GetHashCode());
    }
}
