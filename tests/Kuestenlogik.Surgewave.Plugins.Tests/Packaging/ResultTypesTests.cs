using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Result-objects (<see cref="PluginInstallResult"/>, <see cref="PackageValidationResult"/>,
/// <see cref="ChecksumVerificationResult"/>) are factory-only — the constructors are private
/// to force callers through Success/Failed/Valid/Invalid. Tests verify both branches.
/// </summary>
public sealed class ResultTypesTests
{
    private static PluginManifest SampleManifest() => new()
    {
        Id = "x",
        Name = "X",
        Version = "1.0.0",
        Assemblies = ["x.dll"],
    };

    [Fact]
    public void PluginInstallResult_Succeeded_PopulatesManifestAndPath()
    {
        var manifest = SampleManifest();

        var r = PluginInstallResult.Succeeded(manifest, "/plugins/x");

        Assert.True(r.Success);
        Assert.Equal(manifest, r.Manifest);
        Assert.Equal("/plugins/x", r.InstallPath);
        Assert.False(r.WasUpgrade);
        Assert.Null(r.PreviousVersion);
        Assert.Null(r.Error);
    }

    [Fact]
    public void PluginInstallResult_Succeeded_WithUpgrade_TracksPreviousVersion()
    {
        var manifest = SampleManifest();

        var r = PluginInstallResult.Succeeded(manifest, "/plugins/x", wasUpgrade: true, previousVersion: "0.9.0");

        Assert.True(r.Success);
        Assert.True(r.WasUpgrade);
        Assert.Equal("0.9.0", r.PreviousVersion);
    }

    [Fact]
    public void PluginInstallResult_Failed_CarriesError()
    {
        var r = PluginInstallResult.Failed("nope");

        Assert.False(r.Success);
        Assert.Equal("nope", r.Error);
        Assert.Null(r.Manifest);
        Assert.Null(r.InstallPath);
    }

    [Fact]
    public void PackageValidationResult_Valid_NoErrors()
    {
        var r = PackageValidationResult.Valid(SampleManifest());

        Assert.True(r.IsValid);
        Assert.NotNull(r.Manifest);
        Assert.Empty(r.Errors);
        Assert.Empty(r.Warnings);
    }

    [Fact]
    public void PackageValidationResult_Valid_WithWarnings()
    {
        var r = PackageValidationResult.Valid(SampleManifest(), warnings: ["heads-up"]);

        Assert.True(r.IsValid);
        Assert.Single(r.Warnings, "heads-up");
    }

    [Fact]
    public void PackageValidationResult_InvalidSingleError()
    {
        var r = PackageValidationResult.Invalid("missing manifest");

        Assert.False(r.IsValid);
        Assert.Null(r.Manifest);
        Assert.Single(r.Errors, "missing manifest");
    }

    [Fact]
    public void PackageValidationResult_InvalidMultipleErrors_PreservesOrder()
    {
        var errors = new List<string> { "error 1", "error 2", "error 3" };

        var r = PackageValidationResult.Invalid(errors);

        Assert.False(r.IsValid);
        Assert.Equal(errors, r.Errors);
    }

    [Fact]
    public void ChecksumVerificationResult_RecordEquality_WorksByValue()
    {
        var a = new ChecksumVerificationResult(IsValid: true, ExpectedHash: "ab", ComputedHash: "ab");
        var b = new ChecksumVerificationResult(IsValid: true, ExpectedHash: "ab", ComputedHash: "ab");

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }
}
