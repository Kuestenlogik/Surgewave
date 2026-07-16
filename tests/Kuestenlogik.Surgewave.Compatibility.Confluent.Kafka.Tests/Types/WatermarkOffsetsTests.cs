namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins WatermarkOffsets construction and its "[low, high]" formatting,
/// including symbolic names for special offsets.
/// </summary>
public class WatermarkOffsetsTests
{
    [Fact]
    public void Constructor_SetsLowAndHigh()
    {
        var watermarks = new WatermarkOffsets(new Offset(0), new Offset(100));

        Assert.Equal(new Offset(0), watermarks.Low);
        Assert.Equal(new Offset(100), watermarks.High);
    }

    [Fact]
    public void ToString_FormatsAsRange()
    {
        var watermarks = new WatermarkOffsets(new Offset(0), new Offset(100));
        Assert.Equal("[0, 100]", watermarks.ToString());
    }

    [Fact]
    public void ToString_UsesSpecialOffsetNames()
    {
        var watermarks = new WatermarkOffsets(Offset.Beginning, Offset.End);
        Assert.Equal("[Beginning, End]", watermarks.ToString());
    }
}
