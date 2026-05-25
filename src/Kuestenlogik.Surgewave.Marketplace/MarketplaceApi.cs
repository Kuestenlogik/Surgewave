using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Marketplace.Models;
using Kuestenlogik.Surgewave.Marketplace.Services;
using Kuestenlogik.Surgewave.Marketplace.Storage;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Marketplace;

/// <summary>
/// REST API endpoints for the Surgewave plugin marketplace.
/// </summary>
public static class MarketplaceApi
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };

    public static IEndpointRouteBuilder MapMarketplaceApi(
        this IEndpointRouteBuilder app,
        IPackageStorageService storage,
        IPackageMetadataService metadata,
        MarketplaceSignerOptions? signerOptions = null)
    {
        signerOptions ??= new MarketplaceSignerOptions();
        var api = app.MapGroup("/api/v1").WithTags("Marketplace");

        // Service index
        api.MapGet("/index.json", () => Results.Ok(new
        {
            version = "1.0.0",
            resources = new[]
            {
                new { id = "/api/v1/search", type = "SearchQueryService" },
                new { id = "/api/v1/packages", type = "PackageBaseAddress" },
                new { id = "/api/v1/statistics", type = "StatisticsService" }
            }
        }));

        // Search
        api.MapGet("/search", async (string? q, int? skip, int? take, CancellationToken ct) =>
        {
            var results = await metadata.SearchAsync(q, skip ?? 0, take ?? 20, ct);
            return Results.Ok(new
            {
                totalHits = results.Count,
                data = results
            });
        });

        // Package versions
        api.MapGet("/packages/{id}/index.json", async (string id, CancellationToken ct) =>
        {
            var versions = await metadata.GetVersionsAsync(id, ct);
            if (versions.Count == 0)
                return Results.NotFound(new { error = $"Package '{id}' not found" });

            return Results.Ok(new { versions });
        });

        // Package metadata
        api.MapGet("/packages/{id}/{version}/metadata", async (string id, string version, CancellationToken ct) =>
        {
            var meta = await metadata.GetAsync(id, version, ct);
            if (meta == null)
                return Results.NotFound(new { error = $"Package '{id}' v{version} not found" });

            return Results.Ok(meta);
        });

        // Bundled defaults — answers 'what configuration defaults does this version
        // ship?'. The endpoint reads the .swpkg's plugin.json to find the configured
        // pluginSettings filename, extracts that file, and returns it as JSON.
        // Pairs with the operator-side 'surgewave plugin diff' CLI command and lets
        // the marketplace UI render a side-by-side defaults preview when an upgrade
        // is available — without re-downloading the package.
        api.MapGet("/packages/{id}/{version}/defaults", async (string id, string version, CancellationToken ct) =>
        {
            if (!await storage.ExistsAsync(id, version, ct))
                return Results.NotFound(new { error = $"Package '{id}' v{version} not found" });

            await using var stream = await storage.GetPackageAsync(id, version, ct);
            using var archive = new ZipArchive(stream, ZipArchiveMode.Read);

            var manifestEntry = archive.GetEntry("plugin.json");
            if (manifestEntry == null)
                return Results.NotFound(new { error = "Package missing plugin.json manifest" });

            PluginManifest? manifest;
            await using (var ms = manifestEntry.Open())
            {
                manifest = await JsonSerializer.DeserializeAsync<PluginManifest>(ms, JsonOptions, ct);
            }
            if (manifest is null)
                return Results.UnprocessableEntity(new { error = "plugin.json could not be parsed" });

            var settingsName = string.IsNullOrWhiteSpace(manifest.PluginSettings)
                ? "pluginsettings.json"
                : manifest.PluginSettings;
            var settingsEntry = archive.GetEntry(settingsName);
            if (settingsEntry == null)
            {
                return Results.Ok(new { id, version, settingsFile = (string?)null, content = (object?)null });
            }

            await using var settingsStream = settingsEntry.Open();
            using var settingsDoc = await JsonDocument.ParseAsync(settingsStream, cancellationToken: ct);
            return Results.Ok(new
            {
                id,
                version,
                settingsFile = settingsName,
                content = settingsDoc.RootElement.Clone(),
            });
        });

        // Download .swpkg
        api.MapGet("/packages/{id}/{version}/download", async (string id, string version, CancellationToken ct) =>
        {
            if (!await storage.ExistsAsync(id, version, ct))
                return Results.NotFound(new { error = $"Package '{id}' v{version} not found" });

            await metadata.IncrementDownloadCountAsync(id, version, ct);

            var stream = await storage.GetPackageAsync(id, version, ct);
            return Results.File(stream, "application/octet-stream", $"{id}-{version}.swpkg");
        });

        // Download signature sidecar (.sig for builtin-ecdsa, .cms for sealbolt)
        api.MapGet("/packages/{id}/{version}/signature", async (string id, string version, CancellationToken ct) =>
        {
            var sigStream = await storage.GetSignatureAsync(id, version, ct);
            if (sigStream is null)
                return Results.NotFound(new { error = $"Signature for '{id}' v{version} not found" });

            var ext = await storage.GetSignatureExtensionAsync(id, version, ct) ?? ".sig";
            return Results.File(sigStream, "application/octet-stream", $"{id}-{version}.swpkg{ext}");
        });

        // CycloneDX Software Bill of Materials (extracted from the .swpkg at upload time).
        api.MapGet("/packages/{id}/{version}/sbom", async (string id, string version, CancellationToken ct) =>
        {
            var sbomStream = await storage.GetSbomAsync(id, version, ct);
            if (sbomStream is null)
                return Results.NotFound(new { error = $"SBOM for '{id}' v{version} not found" });

            return Results.File(sbomStream, "application/vnd.cyclonedx+json", $"{id}-{version}-sbom.json");
        });

        // Upload/Publish .swpkg
        api.MapPut("/packages", async (HttpRequest request, CancellationToken ct) =>
        {
            var form = await request.ReadFormAsync(ct);
            var file = form.Files.GetFile("file") ?? (form.Files.Count > 0 ? form.Files[0] : null);

            if (file == null || file.Length == 0)
                return Results.BadRequest(new { error = "No .swpkg file provided. Upload as multipart form with 'file' field." });

            if (!file.FileName.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest(new { error = "File must have .swpkg extension" });

            // Optional sidecar signature (.sig for builtin-ecdsa, .cms for sealbolt)
            var signatureFile = form.Files.GetFile("signature");
            string? signatureExtension = null;
            if (signatureFile is not null && signatureFile.Length > 0)
            {
                signatureExtension = Path.GetExtension(signatureFile.FileName);
                // The upload flow appends the sidecar to {packageFileName}.swpkg, so the user may
                // have uploaded e.g. "foo.swpkg.sig" — strip one layer of extension to normalise.
                if (signatureExtension is ".sig" or ".cms")
                {
                    // ok
                }
                else
                {
                    return Results.BadRequest(new { error = $"Signature file must be .sig or .cms, got '{signatureExtension}'" });
                }
            }
            else if (signerOptions.RequireSignedUploads)
            {
                return Results.BadRequest(new { error = "Unsigned uploads are rejected by marketplace policy (Surgewave:Marketplace:Signing:RequireSignedUploads=true). Supply the signature sidecar as the 'signature' form field." });
            }

            // Save to temp, extract manifest, then store
            var tempPath = Path.Combine(Path.GetTempPath(), $"pluginPackage-upload-{Guid.NewGuid()}.swpkg");
            string? tempSigPath = null;
            try
            {
                await using (var stream = new FileStream(tempPath, FileMode.Create))
                {
                    await file.CopyToAsync(stream, ct);
                }

                if (signatureFile is not null && signatureExtension is not null)
                {
                    tempSigPath = tempPath + signatureExtension;
                    await using var sigStream = new FileStream(tempSigPath, FileMode.Create);
                    await signatureFile.CopyToAsync(sigStream, ct);
                }

                // Read manifest from package
                using (var archive = ZipFile.OpenRead(tempPath))
                {
                    var manifestEntry = archive.GetEntry("plugin.json");
                    if (manifestEntry == null)
                        return Results.BadRequest(new { error = "Package missing plugin.json manifest" });
                }

                PluginManifest manifest;
                using (var archive = ZipFile.OpenRead(tempPath))
                {
                    var manifestEntry = archive.GetEntry("plugin.json")!;
                    await using var manifestStream = manifestEntry.Open();
                    manifest = await JsonSerializer.DeserializeAsync<PluginManifest>(manifestStream, JsonOptions, ct)
                        ?? throw new InvalidOperationException("Failed to parse manifest");
                }

                // Verify signature if provided
                bool isSigned = false;
                string? signerIdentity = null;
                string? signerProvider = null;

                if (signatureFile is not null && signatureExtension is not null)
                {
                    using var registry = PluginPackageSignerRegistry.LoadFrom(signerOptions.PluginsDirectory);
                    ISppSignerProvider? provider;
                    try
                    {
                        provider = registry.GetProvider(signerOptions.SignerName);
                    }
                    catch (KeyNotFoundException ex)
                    {
                        return Results.Problem($"Marketplace signer misconfiguration: {ex.Message}", statusCode: 500);
                    }

                    ISppSigner verifier;
                    try
                    {
                        verifier = provider.Create(signerOptions.SignerOptions);
                    }
                    catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
                    {
                        return Results.Problem($"Marketplace signer options invalid: {ex.Message}", statusCode: 500);
                    }

                    if (!verifier.HasSignature(tempPath))
                    {
                        return Results.BadRequest(new
                        {
                            error = $"Signature sidecar format ({signatureExtension}) does not match the configured '{provider.Name}' provider."
                        });
                    }

                    var result = await verifier.VerifyAsync(tempPath, ct);
                    if (!result.IsValid)
                    {
                        return Results.BadRequest(new
                        {
                            error = $"Signature verification failed ({provider.Name}): {result.Reason}"
                        });
                    }

                    isSigned = true;
                    signerIdentity = result.SignerIdentity;
                    signerProvider = provider.Name;
                }

                // Compute SHA256
                var sha256 = await PackageChecksumCalculator.ComputeAsync(tempPath, ct);

                // Extract the CycloneDX SBOM if one was bundled with the package. Skipped silently
                // when absent so older .swpkg files without SBOM still upload cleanly.
                byte[]? sbomBytes = null;
                using (var sbomArchive = ZipFile.OpenRead(tempPath))
                {
                    var sbomEntry = sbomArchive.GetEntry("sbom.json");
                    if (sbomEntry is not null)
                    {
                        await using var sbomEntryStream = sbomEntry.Open();
                        using var sbomBuffer = new MemoryStream();
                        await sbomEntryStream.CopyToAsync(sbomBuffer, ct);
                        sbomBytes = sbomBuffer.ToArray();
                    }
                }

                // Store the package
                await using (var packageStream = new FileStream(tempPath, FileMode.Open, FileAccess.Read))
                {
                    await storage.SavePackageAsync(manifest.Id, manifest.Version, packageStream, ct);
                }

                // Store the signature sidecar alongside the package if one was provided
                if (tempSigPath is not null && signatureExtension is not null)
                {
                    await using var sigStream = new FileStream(tempSigPath, FileMode.Open, FileAccess.Read);
                    await storage.SaveSignatureAsync(manifest.Id, manifest.Version, signatureExtension, sigStream, ct);
                }

                if (sbomBytes is not null)
                {
                    await storage.SaveSbomAsync(manifest.Id, manifest.Version, sbomBytes, ct);
                }

                var packageSize = new FileInfo(tempPath).Length;

                // Build or update metadata
                var existing = await metadata.GetAsync(manifest.Id, ct: ct);
                var allVersions = existing?.AllVersions ?? [];
                if (!allVersions.Contains(manifest.Version))
                    allVersions = [.. allVersions, manifest.Version];

                var meta = new PackageMetadata
                {
                    Id = manifest.Id,
                    Version = manifest.Version,
                    Name = manifest.Name,
                    Description = manifest.Description,
                    Authors = manifest.Authors,
                    Tags = manifest.Tags,
                    License = manifest.License,
                    ProjectUrl = manifest.ProjectUrl,
                    Sha256 = sha256,
                    PackageSize = packageSize,
                    Assemblies = manifest.Assemblies,
                    PublishedAt = DateTimeOffset.UtcNow,
                    DownloadCount = existing?.DownloadCount ?? 0,
                    AllVersions = allVersions.ToList(),
                    IsSigned = isSigned,
                    SignerIdentity = signerIdentity,
                    SignerProvider = signerProvider,
                    HasSbom = sbomBytes is not null
                };

                await metadata.SaveAsync(meta, ct);

                return Results.Created($"/api/v1/packages/{manifest.Id}/{manifest.Version}/metadata", new
                {
                    id = manifest.Id,
                    version = manifest.Version,
                    sha256,
                    packageSize,
                    isSigned,
                    signerIdentity,
                    signerProvider
                });
            }
            finally
            {
                try { File.Delete(tempPath); } catch { /* ignore */ }
                if (tempSigPath is not null)
                {
                    try { File.Delete(tempSigPath); } catch { /* ignore */ }
                }
            }
        }).DisableAntiforgery();

        // Unlist a package version
        api.MapDelete("/packages/{id}/{version}", async (string id, string version, CancellationToken ct) =>
        {
            var meta = await metadata.GetAsync(id, version, ct);
            if (meta == null)
                return Results.NotFound(new { error = $"Package '{id}' v{version} not found" });

            await storage.DeletePackageAsync(id, version, ct);
            await metadata.DeleteAsync(id, version, ct);

            return Results.NoContent();
        });

        // Statistics
        api.MapGet("/statistics", async (CancellationToken ct) =>
        {
            var stats = await metadata.GetStatisticsAsync(ct);
            return Results.Ok(stats);
        });

        return app;
    }
}
