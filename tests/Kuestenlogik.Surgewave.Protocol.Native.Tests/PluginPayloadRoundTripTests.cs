using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — plugin-registry payloads (GetPlugin, InstallPlugin,
/// plus the shared <see cref="PluginInfoPayload"/> /
/// <see cref="PluginDependencyPayload"/> / <see cref="InstalledPackageInfoPayload"/>
/// building blocks). These back the <c>surgewave plugin install</c> /
/// <c>surgewave plugin get</c> CLI and the Control-UI's marketplace tab.
///
/// PluginInfoPayload is the shape-heaviest payload in the namespace:
/// 17 fields including 6 nullable strings, 4 nested string-array lists,
/// and a nested PluginDependencyPayload list — a worthwhile pin since the
/// install flow round-trips it through the broker on every request.
/// </summary>
public sealed class PluginPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // PluginDependency (atomic building block)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void PluginDependencyPayload_RoundTrip_PreservesAllFields()
    {
        var original = new PluginDependencyPayload { Id = "com.foo.bar", Version = ">=1.2.0", Optional = true };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PluginDependencyPayload.Read(ref r); });

        Assert.Equal("com.foo.bar", parsed.Id);
        Assert.Equal(">=1.2.0", parsed.Version);
        Assert.True(parsed.Optional);
    }

    // ───────────────────────────────────────────────────────────────
    // InstalledPackageInfo (atomic building block)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void InstalledPackageInfoPayload_RoundTrip_PreservesAllFields()
    {
        var original = new InstalledPackageInfoPayload
        {
            PackageId = "io.surgewave.connect.s3",
            Version = "1.0.4",
            WasDependency = false,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return InstalledPackageInfoPayload.Read(ref r); });

        Assert.Equal("io.surgewave.connect.s3", parsed.PackageId);
        Assert.Equal("1.0.4", parsed.Version);
        Assert.False(parsed.WasDependency);
    }

    // ───────────────────────────────────────────────────────────────
    // GetPlugin (Request + Response)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void GetPluginRequestPayload_RoundTrip_WithVersion()
    {
        var original = new GetPluginRequestPayload { PackageId = "io.surgewave.ai.openai", Version = "2.1.0" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetPluginRequestPayload.Read(ref r); });

        Assert.Equal("io.surgewave.ai.openai", parsed.PackageId);
        Assert.Equal("2.1.0", parsed.Version);
    }

    [Fact]
    public void GetPluginRequestPayload_RoundTrip_NullVersion_ResolvesLatest()
    {
        // PackageId without explicit Version means "latest" — pin null
        // round-trips so the broker doesn't misclassify as version="".
        var original = new GetPluginRequestPayload { PackageId = "io.surgewave.ai.openai", Version = null };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetPluginRequestPayload.Read(ref r); });

        Assert.Null(parsed.Version);
    }

    [Fact]
    public void GetPluginResponsePayload_NotFound_RoundTrips()
    {
        // Plugin not in registry — Found=false, Plugin=null. Wire skips
        // the PluginInfo body entirely, so any encoding drift there
        // would still corrupt — pin the skip path.
        var original = new GetPluginResponsePayload { Found = false, Plugin = null };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetPluginResponsePayload.Read(ref r); });

        Assert.False(parsed.Found);
        Assert.Null(parsed.Plugin);
    }

    [Fact]
    public void GetPluginResponsePayload_FoundWithFullPluginInfo_RoundTrips()
    {
        var pluginInfo = new PluginInfoPayload
        {
            PackageId = "io.surgewave.connect.s3",
            Name = "S3 Sink Connector",
            Version = "1.2.3",
            Description = "Stream Kafka topics to S3 buckets",
            Author = "Kuestenlogik",
            License = "Apache-2.0",
            ProjectUrl = "https://github.com/kuestenlogik/connect-s3",
            IconUrl = null, // not all plugins ship an icon
            IsInstalled = true,
            InstalledVersion = "1.2.0", // older than latest available
            DownloadCount = 12_345L,
            ConnectorTypes = new[] { "sink" },
            Tags = new[] { "aws", "s3", "storage" },
            AvailableVersions = new[] { "1.0.0", "1.1.0", "1.2.0", "1.2.3" },
            Dependencies = new[]
            {
                new PluginDependencyPayload { Id = "io.surgewave.connect.core", Version = ">=1.0.0", Optional = false },
            },
            IsSigned = true,
            SignerIdentity = "Kuestenlogik (DE)",
            SignerProvider = "builtin-ecdsa",
        };
        var original = new GetPluginResponsePayload { Found = true, Plugin = pluginInfo };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetPluginResponsePayload.Read(ref r); });

        Assert.True(parsed.Found);
        Assert.NotNull(parsed.Plugin);
        var p = parsed.Plugin!.Value;
        Assert.Equal("io.surgewave.connect.s3", p.PackageId);
        Assert.Equal("1.2.3", p.Version);
        Assert.Equal("1.2.0", p.InstalledVersion);
        Assert.True(p.IsInstalled);
        Assert.Equal(12_345L, p.DownloadCount);
        Assert.Equal(new[] { "sink" }, p.ConnectorTypes);
        Assert.Equal(new[] { "aws", "s3", "storage" }, p.Tags);
        Assert.Equal(4, p.AvailableVersions.Count);
        Assert.Single(p.Dependencies);
        Assert.True(p.IsSigned);
        Assert.Equal("Kuestenlogik (DE)", p.SignerIdentity);
        Assert.Equal("builtin-ecdsa", p.SignerProvider);
        Assert.Null(p.IconUrl);
    }

    [Fact]
    public void PluginInfoPayload_AllNullableUnset_RoundTrips()
    {
        // Minimal community plugin: required fields only, all nullables
        // null. Pin the wire shape for the "no metadata" case.
        var pluginInfo = new PluginInfoPayload
        {
            PackageId = "draft-plugin",
            Name = "Draft",
            Version = "0.0.1",
            Description = null,
            Author = null,
            License = null,
            ProjectUrl = null,
            IconUrl = null,
            IsInstalled = false,
            InstalledVersion = null,
            DownloadCount = 0,
            ConnectorTypes = Array.Empty<string>(),
            Tags = Array.Empty<string>(),
            AvailableVersions = new[] { "0.0.1" },
            Dependencies = Array.Empty<PluginDependencyPayload>(),
            IsSigned = false,
            SignerIdentity = null,
            SignerProvider = null,
        };
        var parsed = RoundTrip(
            pluginInfo.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); pluginInfo.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PluginInfoPayload.Read(ref r); });

        Assert.Null(parsed.Description);
        Assert.Null(parsed.Author);
        Assert.Null(parsed.License);
        Assert.Null(parsed.ProjectUrl);
        Assert.Null(parsed.IconUrl);
        Assert.Null(parsed.InstalledVersion);
        Assert.Null(parsed.SignerIdentity);
        Assert.Null(parsed.SignerProvider);
        Assert.False(parsed.IsSigned);
        Assert.Empty(parsed.ConnectorTypes);
        Assert.Empty(parsed.Tags);
        Assert.Empty(parsed.Dependencies);
        Assert.Single(parsed.AvailableVersions);
    }

    // ───────────────────────────────────────────────────────────────
    // InstallPlugin (Request + Response)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void InstallPluginRequestPayload_RoundTrip_PreservesAllFields()
    {
        var original = new InstallPluginRequestPayload
        {
            PackageId = "io.surgewave.connect.s3",
            Version = "1.2.3",
            IncludeDependencies = true,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return InstallPluginRequestPayload.Read(ref r); });

        Assert.Equal("io.surgewave.connect.s3", parsed.PackageId);
        Assert.Equal("1.2.3", parsed.Version);
        Assert.True(parsed.IncludeDependencies);
    }

    [Fact]
    public void InstallPluginResponsePayload_FullSuccess_RoundTrips()
    {
        var original = new InstallPluginResponsePayload
        {
            IsSuccess = true,
            IsPartialSuccess = false,
            InstalledPackages = new[]
            {
                new InstalledPackageInfoPayload { PackageId = "io.surgewave.connect.s3",   Version = "1.2.3", WasDependency = false },
                new InstalledPackageInfoPayload { PackageId = "io.surgewave.connect.core", Version = "1.1.0", WasDependency = true  },
            },
            Errors = Array.Empty<string>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return InstallPluginResponsePayload.Read(ref r); });

        Assert.True(parsed.IsSuccess);
        Assert.False(parsed.IsPartialSuccess);
        Assert.Equal(2, parsed.InstalledPackages.Count);
        Assert.True(parsed.InstalledPackages[1].WasDependency);
        Assert.Empty(parsed.Errors);
    }

    [Fact]
    public void InstallPluginResponsePayload_PartialFailureWithErrors_RoundTrips()
    {
        var original = new InstallPluginResponsePayload
        {
            IsSuccess = false,
            IsPartialSuccess = true,
            InstalledPackages = new[]
            {
                new InstalledPackageInfoPayload { PackageId = "io.surgewave.connect.core", Version = "1.1.0", WasDependency = true },
            },
            Errors = new[]
            {
                "Dependency 'io.surgewave.connect.s3' failed: signature verification failed (signer identity mismatch)",
                "Rollback successful for core dependency",
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return InstallPluginResponsePayload.Read(ref r); });

        Assert.False(parsed.IsSuccess);
        Assert.True(parsed.IsPartialSuccess);
        Assert.Single(parsed.InstalledPackages);
        Assert.Equal(2, parsed.Errors.Count);
        Assert.Contains("signature verification", parsed.Errors[0]);
    }
}
