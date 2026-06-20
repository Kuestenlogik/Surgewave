using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// REST endpoints for managing the BuiltinEcdsaSigner trust store — the
/// directory of `.pub` files that <see cref="BuiltinEcdsaSigner"/> consults
/// at install-time signature verification. Mirrors the shape of
/// <see cref="PluginRestApi"/> (group route, parameter validation, narrow
/// error payloads). All endpoints return 503 with a configuration hint when
/// no <c>trusted-keys-dir</c> is configured under
/// <c>Surgewave:Plugins:Signer:Options</c>.
/// </summary>
public static class TrustedKeysRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveTrustedKeys(
        this IEndpointRouteBuilder app,
        SignerOptions? signerOptions)
    {
        var trustedKeysDir = signerOptions?.Options.TryGetValue("trusted-keys-dir", out var dir) == true
            ? dir
            : null;

        var group = app.MapGroup("/api/plugins/trusted-keys").WithTags("Plugins-Trust-Store");

        IResult RequireConfigured(out TrustStoreService? svc)
        {
            if (string.IsNullOrWhiteSpace(trustedKeysDir))
            {
                svc = null;
                return Results.Json(
                    new
                    {
                        error = "trust store not configured",
                        hint = "set Surgewave:Plugins:Signer:Options:trusted-keys-dir in appsettings.json or via environment variable.",
                    },
                    statusCode: StatusCodes.Status503ServiceUnavailable);
            }
            svc = new TrustStoreService(trustedKeysDir);
            return Results.Empty; // sentinel — caller checks svc != null
        }

        group.MapGet("/", () =>
        {
            var pre = RequireConfigured(out var svc);
            if (svc is null) return pre;
            return Results.Ok(new
            {
                trustedKeysDir,
                requireSigned = signerOptions?.RequireSignedPackages ?? false,
                providerName = signerOptions?.Name ?? "builtin-ecdsa",
                keys = svc.List().Select(k => new
                {
                    name = k.Name,
                    fingerprint = k.Fingerprint,
                    lastModifiedUtc = k.LastModifiedUtc,
                    sizeBytes = k.SizeBytes,
                }),
            });
        });

        group.MapPost("/upload", async (HttpRequest request, ILogger<TrustStoreService> logger) =>
        {
            var pre = RequireConfigured(out var svc);
            if (svc is null) return pre;

            var form = await request.ReadFormAsync();
            var file = form.Files.GetFile("file");
            var name = form["name"].ToString();

            if (file is null || file.Length == 0)
            {
                return Results.BadRequest(new { error = "No public-key file provided." });
            }
            if (string.IsNullOrWhiteSpace(name))
            {
                // Fall back to the filename without .pub extension if the caller
                // didn't pass an explicit name field. Keeps `curl -F file=@alice.pub`
                // workflows ergonomic.
                name = Path.GetFileNameWithoutExtension(file.FileName);
            }

            try
            {
                await using var stream = file.OpenReadStream();
                var info = await svc.UploadAsync(name, stream);
                logger.LogInformation("Trusted key '{Name}' uploaded ({Fingerprint})", info.Name, info.Fingerprint);
                return Results.Ok(new
                {
                    name = info.Name,
                    fingerprint = info.Fingerprint,
                    lastModifiedUtc = info.LastModifiedUtc,
                    sizeBytes = info.SizeBytes,
                });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { error = ex.Message });
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Trusted key upload rejected");
                return Results.BadRequest(new { error = $"Public key rejected: {ex.Message}" });
            }
        })
        .DisableAntiforgery();

        group.MapDelete("/{name}", (string name, ILogger<TrustStoreService> logger) =>
        {
            var pre = RequireConfigured(out var svc);
            if (svc is null) return pre;

            try
            {
                var removed = svc.Delete(name);
                if (!removed) return Results.NotFound(new { error = $"No trusted key named '{name}'." });
                logger.LogInformation("Trusted key '{Name}' deleted", name);
                return Results.Ok(new { name, deleted = true });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        group.MapPost("/generate", (GenerateKeyRequest req, ILogger<TrustStoreService> logger) =>
        {
            var pre = RequireConfigured(out var svc);
            if (svc is null) return pre;

            try
            {
                var pair = svc.Generate(req.Name);
                logger.LogInformation("Generated trusted key '{Name}' ({Fingerprint})", pair.KeyName, pair.Fingerprint);
                return Results.Ok(new
                {
                    name = pair.KeyName,
                    fingerprint = pair.Fingerprint,
                    publicKeyPem = pair.PublicKeyPem,
                    privateKeyPem = pair.PrivateKeyPem,
                });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { error = ex.Message });
            }
        });

        return app;
    }

    public sealed record GenerateKeyRequest(string Name);
}
