using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for SurgewaveTransportType enum values.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SurgewaveTransportTypeTests
{
    [Fact]
    public void SurgewaveTransportType_Auto_HasValueZero()
    {
        Assert.Equal(0, (int)SurgewaveTransportType.Auto);
    }

    [Fact]
    public void SurgewaveTransportType_Tcp_HasValueOne()
    {
        Assert.Equal(1, (int)SurgewaveTransportType.Tcp);
    }

    [Fact]
    public void SurgewaveTransportType_SharedMemory_HasValueTwo()
    {
        Assert.Equal(2, (int)SurgewaveTransportType.SharedMemory);
    }

    [Fact]
    public void SurgewaveTransportType_DefaultValue_IsAuto()
    {
        // The default (uninitialized) enum value should be Auto (0)
        SurgewaveTransportType defaultValue = default;
        Assert.Equal(SurgewaveTransportType.Auto, defaultValue);
    }

    [Theory]
    [InlineData(SurgewaveTransportType.Auto)]
    [InlineData(SurgewaveTransportType.Tcp)]
    [InlineData(SurgewaveTransportType.SharedMemory)]
    public void SurgewaveTransportType_AllValuesAreDefined(SurgewaveTransportType transportType)
    {
        Assert.True(Enum.IsDefined(transportType));
    }
}
