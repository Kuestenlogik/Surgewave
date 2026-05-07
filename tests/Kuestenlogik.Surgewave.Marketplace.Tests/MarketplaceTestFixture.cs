using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Marketplace;
using Kuestenlogik.Surgewave.Marketplace.Services;
using Kuestenlogik.Surgewave.Marketplace.Storage;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Marketplace.Tests;

/// <summary>
/// Spins up a TestServer-backed Marketplace with a temporary data directory so upload/download
/// flows can run end-to-end in-process. Each fixture instance owns its own data + trust store
/// so tests do not interfere.
/// </summary>
public sealed class MarketplaceTestFixture : IAsyncDisposable, IDisposable
{
    private readonly WebApplication _app;

    public HttpClient Client { get; }
    public string DataDir { get; }
    public string TrustedKeysDir { get; }
    public string PublisherPrivateKeyPath { get; }
    public string PublisherPublicKeyPath { get; }

    public MarketplaceTestFixture(bool requireSignedUploads = false)
    {
        DataDir = Path.Combine(Path.GetTempPath(), "marketplace-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(DataDir);
        TrustedKeysDir = Path.Combine(DataDir, "trusted-keys");
        Directory.CreateDirectory(TrustedKeysDir);

        (PublisherPrivateKeyPath, PublisherPublicKeyPath) =
            BuiltinEcdsaSigner.GenerateKeyPair(DataDir, "test-publisher");
        // Preload the publisher's public key so signed uploads from TestPackageBuilder verify.
        File.Copy(PublisherPublicKeyPath, Path.Combine(TrustedKeysDir, "test-publisher.pub"), overwrite: true);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var storage = new FileSystemPackageStorage(DataDir);
        var metadata = new FileSystemMetadataService(DataDir);
        metadata.InitializeAsync().GetAwaiter().GetResult();

        builder.Services.AddSingleton<IPackageStorageService>(storage);
        builder.Services.AddSingleton<IPackageMetadataService>(metadata);
        builder.Services.AddRoutingCore();

        _app = builder.Build();
        _app.UseRouting();
        _app.MapMarketplaceApi(storage, metadata, new MarketplaceSignerOptions
        {
            SignerName = "builtin-ecdsa",
            SignerOptions = new Dictionary<string, string> { ["trusted-keys-dir"] = TrustedKeysDir },
            PluginsDirectory = DataDir,
            RequireSignedUploads = requireSignedUploads
        });

        _app.StartAsync().GetAwaiter().GetResult();
        Client = _app.GetTestClient();
    }

    /// <summary>
    /// Builds a minimal valid .swpkg package in <see cref="DataDir"/> and returns its path.
    /// The package has a parseable plugin.json + a token lib/*.dll so installs pass validation.
    /// </summary>
    public string BuildPackage(string id = "test.plugin", string version = "1.0.0", bool includeSbom = false)
    {
        var path = Path.Combine(DataDir, $"{id}-{version}.swpkg");
        if (File.Exists(path)) File.Delete(path);

        using (var zip = ZipFile.Open(path, ZipArchiveMode.Create))
        {
            var manifestEntry = zip.CreateEntry("plugin.json");
            using (var s = manifestEntry.Open())
            {
                JsonSerializer.Serialize(s, new PluginManifest
                {
                    Id = id,
                    Name = $"Test plugin {id}",
                    Version = version,
                    Assemblies = [$"{id}.dll"]
                });
            }

            // lib/*.dll is required by the validator — 4 bytes of placeholder is enough.
            var libEntry = zip.CreateEntry($"lib/{id}.dll");
            using (var libStream = libEntry.Open())
            {
                libStream.Write([0x4D, 0x5A, 0x00, 0x00]); // MZ stub
            }

            if (includeSbom)
            {
                var sbomEntry = zip.CreateEntry("sbom.json");
                using var sbomStream = sbomEntry.Open();
                sbomStream.Write(System.Text.Encoding.UTF8.GetBytes(
                    $$"""{"bomFormat":"CycloneDX","specVersion":"1.5","serialNumber":"urn:uuid:{{Guid.NewGuid()}}","version":1,"components":[]}"""));
            }
        }

        return path;
    }

    public async Task<string> BuildSignedPackageAsync(string id = "test.plugin", string version = "1.0.0")
    {
        var packagePath = BuildPackage(id, version);
        var signer = new BuiltinEcdsaSigner(privateKeyPath: PublisherPrivateKeyPath);
        await signer.SignAsync(packagePath);
        return packagePath;
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
        Cleanup();
    }

    public void Dispose()
    {
        Client.Dispose();
        _app.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Cleanup();
    }

    private void Cleanup()
    {
        if (Directory.Exists(DataDir))
        {
            try { Directory.Delete(DataDir, recursive: true); }
            catch { /* best effort */ }
        }
    }
}
