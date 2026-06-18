using System.Text.Json;
using Kuestenlogik.Surgewave.Cli.Commands.Setup;
using Kuestenlogik.Surgewave.Plugins.Marketplace;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Tool.Tests.Commands.Setup;

[Trait("Category", TestCategories.Unit)]
public sealed class AppSettingsGeneratorTests
{
    private static PluginMarketplaceEntry Entry(string id) =>
        new()
        {
            Package = new ConnectorPackageInfo { PackageId = id, Version = "1.0.0", Name = id },
            Category = PluginCategory.StorageEngine,
        };

    private static JsonElement RenderAndParse(SetupAnswers answers)
    {
        var json = AppSettingsGenerator.Render(answers);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    [Fact]
    public void Render_EmptyAnswers_EmitsEmptySurgewaveObject()
    {
        var root = RenderAndParse(new SetupAnswers());
        var surgewave = root.GetProperty("Surgewave");

        Assert.Empty(surgewave.EnumerateObject());
    }

    [Fact]
    public void Render_StoragePicked_EmitsEnginePluginPackageId()
    {
        var root = RenderAndParse(new SetupAnswers { StorageEngine = Entry("Acme.Storage.S3") });

        Assert.Equal("Acme.Storage.S3",
            root.GetProperty("Surgewave").GetProperty("Storage").GetProperty("EnginePlugin").GetString());
    }

    [Fact]
    public void Render_AuthNone_OmitsSecuritySection()
    {
        var root = RenderAndParse(new SetupAnswers { Auth = SetupAuthMethod.None });
        Assert.False(root.GetProperty("Surgewave").TryGetProperty("Security", out _));
    }

    [Theory]
    [InlineData(SetupAuthMethod.SaslPlain, true, false, "PLAIN", false)]
    [InlineData(SetupAuthMethod.SaslScram, true, false, "SCRAM-SHA-256", false)]
    [InlineData(SetupAuthMethod.Tls,       false, true, null, false)]
    [InlineData(SetupAuthMethod.MutualTls, false, true, null, true)]
    public void Render_AuthVariants_EmitMatchingSecurityKeys(
        SetupAuthMethod auth, bool saslExpected, bool tlsExpected, string? mechanism, bool requireClientCert)
    {
        var sec = RenderAndParse(new SetupAnswers { Auth = auth })
            .GetProperty("Surgewave").GetProperty("Security");

        Assert.Equal(saslExpected, sec.GetProperty("SaslEnabled").GetBoolean());
        Assert.Equal(tlsExpected, sec.GetProperty("TlsEnabled").GetBoolean());
        if (mechanism is not null)
            Assert.Equal(mechanism, sec.GetProperty("SaslMechanism").GetString());
        else
            Assert.False(sec.TryGetProperty("SaslMechanism", out _));
        Assert.Equal(requireClientCert, sec.TryGetProperty("RequireClientCertificate", out var rcc) && rcc.GetBoolean());
    }

    [Fact]
    public void Render_TelemetryEnabledWithEndpoint_EmitsTelemetrySection()
    {
        var tel = RenderAndParse(new SetupAnswers
            {
                TelemetryEnabled = true,
                OtlpEndpoint = "https://otel.example:4317",
            })
            .GetProperty("Surgewave").GetProperty("Telemetry");

        Assert.True(tel.GetProperty("Enabled").GetBoolean());
        Assert.Equal("https://otel.example:4317", tel.GetProperty("OtlpEndpoint").GetString());
    }

    [Fact]
    public void Render_TelemetryEnabledWithoutEndpoint_FallsBackToLocalhost()
    {
        var tel = RenderAndParse(new SetupAnswers { TelemetryEnabled = true })
            .GetProperty("Surgewave").GetProperty("Telemetry");

        Assert.Equal("http://localhost:4317", tel.GetProperty("OtlpEndpoint").GetString());
    }

    [Fact]
    public void Render_TelemetryDisabled_OmitsTelemetrySection()
    {
        var root = RenderAndParse(new SetupAnswers { TelemetryEnabled = false });
        Assert.False(root.GetProperty("Surgewave").TryGetProperty("Telemetry", out _));
    }

    [Fact]
    public void Render_IsValidJson_WhenAllSectionsPresent()
    {
        var json = AppSettingsGenerator.Render(new SetupAnswers
        {
            StorageEngine = Entry("Acme.Storage.S3"),
            Auth = SetupAuthMethod.MutualTls,
            TelemetryEnabled = true,
            OtlpEndpoint = "https://otel.example:4317",
        });

        var parsed = JsonDocument.Parse(json);
        var sw = parsed.RootElement.GetProperty("Surgewave");
        Assert.True(sw.TryGetProperty("Storage", out _));
        Assert.True(sw.TryGetProperty("Security", out _));
        Assert.True(sw.TryGetProperty("Telemetry", out _));
    }
}
