namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

public class OffsetTests
{
    [Fact]
    public void Constructor_WithValue_SetsValue()
    {
        var offset = new Offset(100);
        Assert.Equal(100, offset.Value);
    }

    [Fact]
    public void Beginning_HasSpecialValue()
    {
        Assert.Equal(-2, Offset.Beginning.Value);
    }

    [Fact]
    public void End_HasSpecialValue()
    {
        Assert.Equal(-1, Offset.End.Value);
    }

    [Fact]
    public void Stored_HasSpecialValue()
    {
        Assert.Equal(-1000, Offset.Stored.Value);
    }

    [Fact]
    public void Unset_HasSpecialValue()
    {
        Assert.Equal(-1001, Offset.Unset.Value);
    }

    [Fact]
    public void Equality_SameValue_ReturnsTrue()
    {
        var o1 = new Offset(50);
        var o2 = new Offset(50);
        Assert.Equal(o1, o2);
        Assert.True(o1 == o2);
    }

    [Fact]
    public void Equality_DifferentValue_ReturnsFalse()
    {
        var o1 = new Offset(50);
        var o2 = new Offset(100);
        Assert.NotEqual(o1, o2);
        Assert.True(o1 != o2);
    }

    [Fact]
    public void ImplicitConversion_FromLong_Works()
    {
        Offset o = 200L;
        Assert.Equal(200, o.Value);
    }

    [Fact]
    public void ImplicitConversion_ToLong_Works()
    {
        var offset = new Offset(300);
        long value = offset;
        Assert.Equal(300, value);
    }

    [Fact]
    public void IsSpecial_ForSpecialOffsets_ReturnsTrue()
    {
        Assert.True(Offset.Beginning.IsSpecial);
        Assert.True(Offset.End.IsSpecial);
        Assert.True(Offset.Stored.IsSpecial);
        Assert.True(Offset.Unset.IsSpecial);
    }

    [Fact]
    public void IsSpecial_ForNormalOffset_ReturnsFalse()
    {
        var offset = new Offset(100);
        Assert.False(offset.IsSpecial);
    }

    [Fact]
    public void ToString_ReturnsValue()
    {
        var offset = new Offset(42);
        Assert.Equal("42", offset.ToString());
    }
}
