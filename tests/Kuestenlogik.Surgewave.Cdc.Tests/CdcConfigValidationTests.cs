using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for <see cref="CdcConfig.Validate"/>. Pins the cross-property rules
/// (connection string only required when CDC is enabled, no blank table entries)
/// and the data-annotation constraints on slot name, publication name, and ack interval.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcConfigValidationTests
{
    [Fact]
    public void Validate_DefaultConfig_HasNoErrors()
    {
        var config = new CdcConfig();

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_EnabledWithoutConnectionString_ReportsError()
    {
        var config = new CdcConfig { Enabled = true, ConnectionString = "" };

        var errors = config.Validate();

        Assert.Contains("ConnectionString: required when CDC is enabled.", errors);
    }

    [Fact]
    public void Validate_EnabledWithWhitespaceConnectionString_ReportsError()
    {
        var config = new CdcConfig { Enabled = true, ConnectionString = "   " };

        var errors = config.Validate();

        Assert.Contains("ConnectionString: required when CDC is enabled.", errors);
    }

    [Fact]
    public void Validate_EnabledWithConnectionString_HasNoErrors()
    {
        var config = new CdcConfig
        {
            Enabled = true,
            ConnectionString = "Host=localhost;Database=db;Username=u;Password=p"
        };

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_DisabledWithoutConnectionString_DoesNotRequireConnectionString()
    {
        var config = new CdcConfig { Enabled = false, ConnectionString = "" };

        var errors = config.Validate();

        Assert.DoesNotContain(errors, e => e.Contains("ConnectionString", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_EmptySlotName_ReportsError()
    {
        var config = new CdcConfig { SlotName = "" };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("SlotName", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_EmptyPublicationName_ReportsError()
    {
        var config = new CdcConfig { PublicationName = "" };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("PublicationName", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_BlankTableEntry_ReportsError(string blankEntry)
    {
        var config = new CdcConfig { Tables = ["public.orders", blankEntry] };

        var errors = config.Validate();

        Assert.Contains("Tables: must not contain empty entries.", errors);
    }

    [Fact]
    public void Validate_ValidTableEntries_HasNoErrors()
    {
        var config = new CdcConfig { Tables = ["public.orders", "inventory.items", "users"] };

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(3601)]
    public void Validate_AckIntervalOutOfRange_ReportsError(int ackInterval)
    {
        var config = new CdcConfig { AckIntervalSeconds = ackInterval };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("AckIntervalSeconds", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(3600)]
    public void Validate_AckIntervalWithinRange_HasNoErrors(int ackInterval)
    {
        var config = new CdcConfig { AckIntervalSeconds = ackInterval };

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_MultipleViolations_ReportsAllErrors()
    {
        var config = new CdcConfig
        {
            Enabled = true,
            ConnectionString = "",
            SlotName = "",
            AckIntervalSeconds = 0,
            Tables = [""]
        };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("ConnectionString", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("SlotName", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("AckIntervalSeconds", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("Tables", StringComparison.Ordinal));
        Assert.True(errors.Count >= 4);
    }
}
