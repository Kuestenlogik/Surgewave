using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Configuration;

/// <summary>
/// Tests for the central <see cref="ConfigValidator"/> infrastructure introduced for Surgewave
/// configuration validation. Verifies the DataAnnotations evaluator, the
/// <see cref="IValidatableConfig"/> contract and the fail-fast helper.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConfigValidatorTests
{
    [Fact]
    public void ValidateDataAnnotations_ValidConfig_ReturnsEmpty()
    {
        var config = new SampleConfig
        {
            Name = "test",
            Port = 8080,
            Ratio = 0.5
        };

        var errors = ConfigValidator.ValidateDataAnnotations(config);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateDataAnnotations_MissingRequiredString_ReportsError()
    {
        var config = new SampleConfig
        {
            Name = "",
            Port = 8080,
            Ratio = 0.5
        };

        var errors = ConfigValidator.ValidateDataAnnotations(config);

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains(nameof(SampleConfig.Name)));
    }

    [Fact]
    public void ValidateDataAnnotations_PortOutOfRange_ReportsError()
    {
        var config = new SampleConfig
        {
            Name = "test",
            Port = 100_000, // outside 1..65535
            Ratio = 0.5
        };

        var errors = ConfigValidator.ValidateDataAnnotations(config);

        Assert.Contains(errors, e => e.Contains(nameof(SampleConfig.Port)));
    }

    [Fact]
    public void ValidateDataAnnotations_RatioOutsideZeroOne_ReportsError()
    {
        var config = new SampleConfig
        {
            Name = "test",
            Port = 8080,
            Ratio = 1.5 // outside 0..1
        };

        var errors = ConfigValidator.ValidateDataAnnotations(config);

        Assert.Contains(errors, e => e.Contains(nameof(SampleConfig.Ratio)));
    }

    [Fact]
    public void Validate_ConfigWithCrossPropertyRule_ReportsBothPasses()
    {
        // MinX > MaxX is a cross-property violation that DataAnnotations alone can't catch.
        var config = new SampleConfigWithCrossRule { MinX = 100, MaxX = 50 };

        var errors = config.Validate();

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains("MinX") && e.Contains("MaxX"));
    }

    [Fact]
    public void Validate_ValidCrossPropertyConfig_ReturnsEmpty()
    {
        var config = new SampleConfigWithCrossRule { MinX = 10, MaxX = 100 };

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void ThrowIfInvalid_InvalidConfig_ThrowsConfigValidationException()
    {
        var config = new SampleConfig
        {
            Name = "",
            Port = 0,
            Ratio = 5.0
        };

        var ex = Assert.Throws<ConfigValidationException>(() => ConfigValidator.ThrowIfInvalid(config));

        Assert.Equal(typeof(SampleConfig), ex.ConfigType);
        Assert.NotEmpty(ex.Errors);
        Assert.Contains(nameof(SampleConfig), ex.Message);
    }

    [Fact]
    public void ThrowIfInvalid_ValidConfig_DoesNotThrow()
    {
        var config = new SampleConfig
        {
            Name = "ok",
            Port = 8080,
            Ratio = 0.5
        };

        // Should not throw
        ConfigValidator.ThrowIfInvalid(config);
    }

    [Fact]
    public void ValidateDataAnnotations_NullArgument_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ConfigValidator.ValidateDataAnnotations(null!));
    }

    // ── Sample configs used by the tests above ──────────────────────────────

    private sealed class SampleConfig : IValidatableConfig
    {
        [Required]
        [MinLength(1)]
        public required string Name { get; init; }

        [Range(1, 65535)]
        public int Port { get; init; }

        [Range(0.0, 1.0)]
        public double Ratio { get; init; }

        public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
    }

    private sealed class SampleConfigWithCrossRule : IValidatableConfig
    {
        [Range(0, int.MaxValue)]
        public int MinX { get; init; }

        [Range(0, int.MaxValue)]
        public int MaxX { get; init; }

        public IReadOnlyList<string> Validate()
        {
            var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));
            if (MinX >= MaxX)
                errors.Add($"{nameof(MinX)} ({MinX}) must be less than {nameof(MaxX)} ({MaxX}).");
            return errors;
        }
    }
}
