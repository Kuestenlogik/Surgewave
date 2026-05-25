using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Builds a CycloneDX 1.5 Software Bill of Materials for a Surgewave plugin package. The SBOM is
/// produced directly from the build output's published directory layout (<c>plugin.json</c>
/// plus the <c>lib/</c> and <c>deps/</c> conventions Surgewave uses when packing a <c>.swpkg</c>) so
/// it records what actually ships, not what the source-level project file declared.
/// </summary>
/// <remarks>
/// <para>
/// Emits: the top-level plugin component in <c>metadata.component</c>, each embedded assembly
/// (<c>manifest.Assemblies</c> entries in <c>lib/</c>) as a component with a SHA-256 hash, and
/// each transitive dependency DLL found under <c>deps/</c> as a component with its assembly-
/// version metadata and SHA-256 hash.
/// </para>
/// <para>
/// Deliberately minimal: no VEX, no licenses, no <c>purl</c> normalisation beyond a best-effort
/// <c>pkg:nuget/…</c> guess from the DLL filename. Callers that need richer SBOMs can post-
/// process the emitted JSON — it is a valid CycloneDX 1.5 document that extension tools accept.
/// </para>
/// </remarks>
public static class SbomGenerator
{
    public const string SbomFileName = "sbom.json";
    private const string SpecVersion = "1.5";
    private const string ToolVendor = "Kuestenlogik.Surgewave";
    private const string ToolName = "Kuestenlogik.Surgewave.Plugins.Packaging";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    /// <summary>
    /// Produces the CycloneDX JSON bytes for a plugin whose publish output lives under
    /// <paramref name="buildOutputDir"/>, using <paramref name="manifest"/> for the top-level
    /// component descriptor. The lib/deps split follows the same layout
    /// <see cref="PluginPackageManager.PackAsync"/> writes into the <c>.swpkg</c> archive.
    /// </summary>
    public static byte[] Build(string buildOutputDir, PluginManifest manifest, DateTimeOffset? timestamp = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(buildOutputDir);
        ArgumentNullException.ThrowIfNull(manifest);

        var now = (timestamp ?? DateTimeOffset.UtcNow).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
        var pluginRef = $"pkg:surgewave/{manifest.Id}@{manifest.Version}";

        var components = new JsonArray();

        // Embedded assemblies (the plugin's own DLLs that land under lib/ in the archive).
        foreach (var assemblyName in manifest.Assemblies)
        {
            var path = Path.Combine(buildOutputDir, assemblyName);
            if (!File.Exists(path)) continue;

            components.Add(ComponentForAssembly(path, bomRefPrefix: "lib", scopeRequired: true));
        }

        // Transitive dependencies (everything the pack step puts under deps/). We scan the
        // build output directly rather than the .deps.json graph because the .swpkg only ships
        // whatever the publish step emitted; deps.json can promise packages that were trimmed.
        var assemblySet = new HashSet<string>(manifest.Assemblies, StringComparer.OrdinalIgnoreCase);
        foreach (var dll in Directory.EnumerateFiles(buildOutputDir, "*.dll"))
        {
            var name = Path.GetFileName(dll);
            if (assemblySet.Contains(name)) continue;
            if (name.StartsWith(SurgewavePackageConventions.HostAssemblyPrefix, StringComparison.Ordinal)) continue;

            components.Add(ComponentForAssembly(dll, bomRefPrefix: "deps", scopeRequired: false));
        }

        var bom = new JsonObject
        {
            ["bomFormat"] = "CycloneDX",
            ["specVersion"] = SpecVersion,
            ["serialNumber"] = $"urn:uuid:{Guid.NewGuid()}",
            ["version"] = 1,
            ["metadata"] = new JsonObject
            {
                ["timestamp"] = now,
                ["tools"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["vendor"] = ToolVendor,
                        ["name"] = ToolName,
                        ["version"] = typeof(SbomGenerator).Assembly.GetName().Version?.ToString(3) ?? "0.1.0"
                    }
                },
                ["component"] = new JsonObject
                {
                    ["type"] = "application",
                    ["bom-ref"] = pluginRef,
                    ["name"] = manifest.Name,
                    ["version"] = manifest.Version,
                    ["description"] = manifest.Description,
                    ["purl"] = pluginRef
                }
            },
            ["components"] = components
        };

        return JsonSerializer.SerializeToUtf8Bytes(bom, JsonOptions);
    }

    private static JsonObject ComponentForAssembly(string assemblyPath, string bomRefPrefix, bool scopeRequired)
    {
        var fileName = Path.GetFileName(assemblyPath);
        var nameNoExt = Path.GetFileNameWithoutExtension(assemblyPath);
        var version = TryReadAssemblyVersion(assemblyPath) ?? "0.0.0";

        var component = new JsonObject
        {
            ["type"] = "library",
            ["bom-ref"] = $"{bomRefPrefix}/{fileName}",
            ["name"] = nameNoExt,
            ["version"] = version,
            ["purl"] = $"pkg:nuget/{nameNoExt}@{version}",
            ["scope"] = scopeRequired ? "required" : "optional",
            ["hashes"] = new JsonArray
            {
                new JsonObject
                {
                    ["alg"] = "SHA-256",
                    ["content"] = Sha256Hex(assemblyPath)
                }
            }
        };
        return component;
    }

    private static string? TryReadAssemblyVersion(string assemblyPath)
    {
        try
        {
            return AssemblyName.GetAssemblyName(assemblyPath).Version?.ToString(3);
        }
        catch
        {
            // Native images, obfuscated assemblies, or non-.NET binaries can throw here.
            // SBOM should still record the hash — leave the version null and let the caller
            // fall back to 0.0.0 to keep the CycloneDX doc valid.
            return null;
        }
    }

    private static string Sha256Hex(string filePath)
    {
        using var stream = File.OpenRead(filePath);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
