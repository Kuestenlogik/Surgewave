using System.IO.Compression;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Manages Surgewave plugin packages — installation, uninstallation, packaging, and validation.
/// </summary>
public sealed class PluginPackageManager
{
    private const string ManifestFileName = SurgewavePackageConventions.ManifestFileName;
    private const string PluginSettingsFileName = SurgewavePackageConventions.PluginSettingsFileName;
    private const string LibDirectory = "lib";
    private const string DepsDirectory = "deps";
    private const string PackageExtension = ".swpkg";
    private const string SharedDirectory = "shared";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };

    private readonly ILogger<PluginPackageManager>? _logger;

    public PluginPackageManager(ILogger<PluginPackageManager>? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Installs a plugin package to the specified plugins directory.
    /// </summary>
    /// <param name="verifier">
    /// Optional signature verifier. If <c>null</c> and <paramref name="requireSigned"/> is <c>true</c>,
    /// a <see cref="BuiltinEcdsaSigner"/> over <c>{pluginsDir}/trusted-keys</c> is used as fallback.
    /// </param>
    public async Task<PluginInstallResult> InstallAsync(
        string packagePath,
        string pluginsDir,
        bool force = false,
        string? expectedSha256 = null,
        ISppSigner? verifier = null,
        bool requireSigned = false,
        CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Installing plugin from {PackagePath}", packagePath);

        if (!string.IsNullOrEmpty(expectedSha256))
        {
            var checksumResult = await PackageChecksumCalculator.VerifyAsync(packagePath, expectedSha256, cancellationToken);
            if (!checksumResult.IsValid)
            {
                return PluginInstallResult.Failed(
                    $"SHA256 checksum mismatch. Expected: {checksumResult.ExpectedHash}, Got: {checksumResult.ComputedHash}");
            }

            _logger?.LogInformation("SHA256 checksum verified for {PackagePath}", packagePath);
        }

        // Signature verification — only runs when the caller asks for it (verifier != null
        // or requireSigned). Unsigned packages pass silently otherwise, matching the default
        // behaviour during development.
        if (verifier is not null || requireSigned)
        {
            verifier ??= new BuiltinEcdsaSigner(trustedKeysDir: Path.Combine(pluginsDir, "trusted-keys"));

            if (verifier.HasSignature(packagePath))
            {
                var result = await verifier.VerifyAsync(packagePath, cancellationToken);
                if (result.IsValid)
                {
                    _logger?.LogInformation("Package signature verified by {Provider} (signed by: {Signer})",
                        verifier.Name, result.SignerIdentity);
                }
                else
                {
                    return PluginInstallResult.Failed(
                        $"Signature verification failed ({verifier.Name}): {result.Reason}");
                }
            }
            else if (requireSigned)
            {
                return PluginInstallResult.Failed(
                    "Package is unsigned and Surgewave:Plugins:RequireSignedPackages is enabled. " +
                    "Only signed packages can be installed.");
            }
            else
            {
                _logger?.LogWarning("Package is unsigned — skipping signature verification");
            }
        }

        var validation = await ValidateAsync(packagePath, cancellationToken);
        if (!validation.IsValid)
        {
            return PluginInstallResult.Failed(string.Join("; ", validation.Errors));
        }

        var manifest = validation.Manifest!;
        var pluginDir = Path.Combine(pluginsDir, manifest.Id);

        string? previousVersion = null;
        var wasUpgrade = false;

        if (Directory.Exists(pluginDir))
        {
            var existingManifest = await TryReadManifestFromDirectoryAsync(pluginDir, cancellationToken);
            if (existingManifest != null)
            {
                previousVersion = existingManifest.Version;

                if (!force)
                {
                    return PluginInstallResult.Failed(
                        $"Plugin {manifest.Id} v{previousVersion} is already installed. Use --force to upgrade.");
                }

                wasUpgrade = true;
                _logger?.LogInformation("Upgrading {PluginId} from v{OldVersion} to v{NewVersion}",
                    manifest.Id, previousVersion, manifest.Version);
            }

            Directory.Delete(pluginDir, recursive: true);
        }

        Directory.CreateDirectory(pluginDir);

        try
        {
            using var archive = ZipFile.OpenRead(packagePath);

            // The manifest's pluginSettings field tells the install side which top-level
            // entry (besides the standard manifest/README/LICENSE/icon set) is allowed
            // through GetTargetPath. Default: pluginsettings.json. Anything else is dropped.
            var pluginSettingsName = string.IsNullOrWhiteSpace(manifest.PluginSettings)
                ? PluginSettingsFileName
                : manifest.PluginSettings;

            foreach (var entry in archive.Entries)
            {
                if (string.IsNullOrEmpty(entry.Name))
                    continue;

                var targetPath = GetTargetPath(pluginDir, entry.FullName, pluginSettingsName);
                if (targetPath == null)
                    continue;

                var targetDir = Path.GetDirectoryName(targetPath);
                if (!string.IsNullOrEmpty(targetDir))
                {
                    Directory.CreateDirectory(targetDir);
                }

                entry.ExtractToFile(targetPath, overwrite: true);
                _logger?.LogDebug("Extracted: {Entry} -> {Target}", entry.FullName, targetPath);
            }

            _logger?.LogInformation("Installed {PluginId} v{Version}", manifest.Id, manifest.Version);

            return PluginInstallResult.Succeeded(manifest, pluginDir, wasUpgrade, previousVersion);
        }
        catch (Exception ex)
        {
            if (Directory.Exists(pluginDir))
            {
                try { Directory.Delete(pluginDir, recursive: true); }
                catch { /* Ignore cleanup errors */ }
            }

            _logger?.LogError(ex, "Failed to install plugin from {PackagePath}", packagePath);
            return PluginInstallResult.Failed($"Installation failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Uninstalls a plugin by its ID.
    /// </summary>
    public Task<bool> UninstallAsync(string pluginId, string pluginsDir)
    {
        var pluginDir = Path.Combine(pluginsDir, pluginId);

        if (!Directory.Exists(pluginDir))
        {
            _logger?.LogWarning("Plugin {PluginId} is not installed", pluginId);
            return Task.FromResult(false);
        }

        try
        {
            Directory.Delete(pluginDir, recursive: true);
            _logger?.LogInformation("Uninstalled plugin {PluginId}", pluginId);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to uninstall plugin {PluginId}", pluginId);
            return Task.FromResult(false);
        }
    }

    /// <summary>
    /// Lists all installed plugins.
    /// </summary>
    public async IAsyncEnumerable<InstalledPlugin> GetInstalledPluginsAsync(
        string pluginsDir,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(pluginsDir))
            yield break;

        foreach (var dir in Directory.GetDirectories(pluginsDir))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var manifest = await TryReadManifestFromDirectoryAsync(dir, cancellationToken);
            if (manifest != null)
            {
                yield return new InstalledPlugin
                {
                    Id = manifest.Id,
                    Name = manifest.Name,
                    Version = manifest.Version,
                    InstallPath = dir,
                    Manifest = manifest
                };
            }
        }
    }

    /// <summary>
    /// Creates a plugin package from build output.
    /// </summary>
    /// <param name="signer">Optional signer. When non-null, a detached signature is written alongside the package.</param>
    public async Task<string> PackAsync(
        string buildOutputDir,
        string? manifestPath,
        string outputPath,
        ISppSigner? signer = null,
        CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Creating package from {BuildOutput}", buildOutputDir);

        // Manifest is required — no guessing
        var existingManifest = Path.Combine(buildOutputDir, "plugin.json");
        if (manifestPath == null && File.Exists(existingManifest))
            manifestPath = existingManifest;

        if (manifestPath == null || !File.Exists(manifestPath))
        {
            throw new InvalidOperationException(
                $"plugin.json not found in {buildOutputDir}. " +
                "Every plugin must have a manifest. Use --manifest to specify the path, " +
                "or place plugin.json in the build output directory.");
        }

        var manifestJson = File.ReadAllText(manifestPath);
        var parsedManifest = System.Text.Json.JsonSerializer.Deserialize<PluginManifest>(manifestJson, JsonOptions);

        var connectorDll = parsedManifest?.Assemblies
            .Select(a => Path.Combine(buildOutputDir, a))
            .FirstOrDefault(File.Exists);

        if (connectorDll == null)
        {
            throw new InvalidOperationException(
                $"No assembly from manifest assemblies found in {buildOutputDir}. " +
                "Check that the assemblies listed in plugin.json exist in the build output.");
        }

        PluginManifest manifest;
        if (manifestPath != null && File.Exists(manifestPath))
        {
            var json = await File.ReadAllTextAsync(manifestPath, cancellationToken);
            manifest = JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions)
                ?? throw new InvalidOperationException("Failed to parse manifest");
        }
        else
        {
            manifest = GenerateManifest(connectorDll);
        }

        // outputPath is the target directory; create it and place the package inside.
        Directory.CreateDirectory(outputPath);

        var packageFileName = $"{manifest.Id}-{manifest.Version}{PackageExtension}";
        var packagePath = Path.Combine(outputPath, packageFileName);

        if (File.Exists(packagePath))
        {
            File.Delete(packagePath);
        }

        using (var archive = ZipFile.Open(packagePath, ZipArchiveMode.Create))
        {
            var manifestEntry = archive.CreateEntry(ManifestFileName);
            await using (var stream = manifestEntry.Open())
            {
                await JsonSerializer.SerializeAsync(stream, manifest, JsonOptions, cancellationToken);
            }

            // Add all assemblies listed in manifest to lib/
            foreach (var assemblyName in manifest.Assemblies)
            {
                var assemblyPath = Path.Combine(buildOutputDir, assemblyName);
                if (File.Exists(assemblyPath))
                {
                    archive.CreateEntryFromFile(assemblyPath, $"{LibDirectory}/{assemblyName}");

                    var depsJson = Path.ChangeExtension(assemblyPath, ".deps.json");
                    if (File.Exists(depsJson))
                    {
                        archive.CreateEntryFromFile(depsJson, $"{LibDirectory}/{Path.GetFileName(depsJson)}");
                    }
                }
            }

            var assemblySet = new HashSet<string>(manifest.Assemblies, StringComparer.OrdinalIgnoreCase);
            foreach (var dll in Directory.GetFiles(buildOutputDir, "*.dll"))
            {
                var name = Path.GetFileName(dll);
                if (!name.StartsWith(SurgewavePackageConventions.HostAssemblyPrefix, StringComparison.Ordinal) && !assemblySet.Contains(name))
                {
                    archive.CreateEntryFromFile(dll, $"{DepsDirectory}/{name}");
                }
            }

            var readme = Path.Combine(buildOutputDir, "..", "README.md");
            if (File.Exists(readme))
            {
                archive.CreateEntryFromFile(readme, "README.md");
            }

            var license = Path.Combine(buildOutputDir, "..", "LICENSE");
            if (File.Exists(license))
            {
                archive.CreateEntryFromFile(license, "LICENSE");
            }

            // Add icon if referenced in manifest
            if (!string.IsNullOrEmpty(manifest.Icon))
            {
                var iconPath = Path.Combine(buildOutputDir, "..", manifest.Icon);
                if (!File.Exists(iconPath))
                    iconPath = Path.Combine(buildOutputDir, manifest.Icon);
                if (File.Exists(iconPath))
                    archive.CreateEntryFromFile(iconPath, manifest.Icon);
            }
            else
            {
                // Auto-detect icon.png or icon.svg in project root
                foreach (var iconName in new[] { "icon.png", "icon.svg" })
                {
                    var iconPath = Path.Combine(buildOutputDir, "..", iconName);
                    if (File.Exists(iconPath))
                    {
                        archive.CreateEntryFromFile(iconPath, iconName);
                        break;
                    }
                }
            }

            // Bundled default settings: explicit manifest path wins, otherwise auto-detect
            // pluginsettings.json next to the manifest. The original filename is preserved
            // inside the archive so the install side can extract it under whatever name the
            // plugin author chose; the broker reads it back via the manifest's pluginSettings
            // field, so any name works as long as the manifest agrees.
            var settingsRelative = string.IsNullOrWhiteSpace(manifest.PluginSettings)
                ? PluginSettingsFileName
                : manifest.PluginSettings;
            // Defence-in-depth: filenames coming out of the manifest land at the archive root
            // and later at the plugin install root. Reject any path separators so a malicious
            // manifest cannot escape via "../foo" or "subdir/foo".
            if (settingsRelative.Contains('/', StringComparison.Ordinal) ||
                settingsRelative.Contains('\\', StringComparison.Ordinal) ||
                settingsRelative.Contains("..", StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Manifest pluginSettings='{settingsRelative}' must be a plain filename, not a path.");
            }
            var settingsCandidate = Path.Combine(buildOutputDir, settingsRelative);
            if (!File.Exists(settingsCandidate))
            {
                settingsCandidate = Path.Combine(buildOutputDir, "..", settingsRelative);
            }
            if (File.Exists(settingsCandidate))
            {
                archive.CreateEntryFromFile(settingsCandidate, settingsRelative);
            }
            else if (!string.IsNullOrWhiteSpace(manifest.PluginSettings))
            {
                throw new InvalidOperationException(
                    $"Manifest declares pluginSettings='{manifest.PluginSettings}' but the file was not found in {buildOutputDir} or its parent.");
            }

            // CycloneDX SBOM goes in alongside the manifest so downstream consumers (marketplace,
            // install-time auditors, SLSA verifiers) can enumerate contents without unpacking
            // the archive. Built from the *build output*, not the in-archive layout, so the
            // hashes can be verified against a recomputed archive listing by the reader.
            var sbomBytes = SbomGenerator.Build(buildOutputDir, manifest);
            var sbomEntry = archive.CreateEntry(SbomGenerator.SbomFileName);
            await using (var sbomStream = sbomEntry.Open())
            {
                await sbomStream.WriteAsync(sbomBytes, cancellationToken);
            }
        }

        var sha256 = await PackageChecksumCalculator.ComputeAsync(packagePath, cancellationToken);
        var checksumPath = packagePath + ".sha256";
        await File.WriteAllTextAsync(checksumPath, $"{sha256}  {Path.GetFileName(packagePath)}", cancellationToken);

        if (signer is not null)
        {
            var sigPath = await signer.SignAsync(packagePath, cancellationToken);
            _logger?.LogInformation("Signed package with {Provider}: {SigPath}", signer.Name, sigPath);
        }

        _logger?.LogInformation("Created package: {PackagePath} (SHA256: {Sha256})", packagePath, sha256);
        return packagePath;
    }

    /// <summary>
    /// Validates a plugin package.
    /// </summary>
    public async Task<PackageValidationResult> ValidateAsync(
        string packagePath,
        CancellationToken cancellationToken = default)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (!File.Exists(packagePath))
            return PackageValidationResult.Invalid($"Package not found: {packagePath}");

        if (!packagePath.EndsWith(PackageExtension, StringComparison.OrdinalIgnoreCase))
            warnings.Add($"Package file does not have {PackageExtension} extension");

        try
        {
            using var archive = ZipFile.OpenRead(packagePath);

            var manifestEntry = archive.GetEntry(ManifestFileName);
            if (manifestEntry == null)
                return PackageValidationResult.Invalid($"Missing {ManifestFileName} in package");

            PluginManifest manifest;
            await using (var stream = manifestEntry.Open())
            {
                manifest = await JsonSerializer.DeserializeAsync<PluginManifest>(stream, JsonOptions, cancellationToken)
                    ?? throw new InvalidOperationException("Failed to parse manifest");
            }

            if (string.IsNullOrWhiteSpace(manifest.Id))
                errors.Add("Manifest missing required field: id");
            if (string.IsNullOrWhiteSpace(manifest.Name))
                errors.Add("Manifest missing required field: name");
            if (string.IsNullOrWhiteSpace(manifest.Version))
                errors.Add("Manifest missing required field: version");

            if (manifest.Assemblies.Length == 0)
            {
                errors.Add("Manifest missing required field: assemblies (must list at least one assembly to scan)");
            }

            var hasLib = archive.Entries.Any(e => e.FullName.StartsWith($"{LibDirectory}/", StringComparison.OrdinalIgnoreCase));
            var hasShared = archive.Entries.Any(e => e.FullName.StartsWith($"{SharedDirectory}/", StringComparison.OrdinalIgnoreCase));
            if (!hasLib && !hasShared)
                errors.Add($"Package missing {LibDirectory}/ or {SharedDirectory}/ directory with assemblies");

            if (errors.Count > 0)
                return PackageValidationResult.Invalid(errors, warnings);

            return PackageValidationResult.Valid(manifest, warnings);
        }
        catch (InvalidDataException)
        {
            return PackageValidationResult.Invalid("Invalid ZIP archive");
        }
        catch (JsonException ex)
        {
            return PackageValidationResult.Invalid($"Invalid manifest JSON: {ex.Message}");
        }
        catch (Exception ex)
        {
            return PackageValidationResult.Invalid($"Validation error: {ex.Message}");
        }
    }

    private static PluginManifest GenerateManifest(string assemblyPath)
    {
        var assemblyName = Path.GetFileNameWithoutExtension(assemblyPath);
        var assembly = Assembly.LoadFrom(assemblyPath);
        var version = assembly.GetName().Version?.ToString(3) ?? "1.0.0";

        var assemblyFileName = Path.GetFileName(assemblyPath);
        return new PluginManifest
        {
            Id = assemblyName,
            Name = GetDisplayName(assemblyName
                .Replace("Kuestenlogik.Surgewave.Connector.", "")
                .Replace("Kuestenlogik.Surgewave.Governance.", "")
                .Replace("Kuestenlogik.Surgewave.Storage.Engine.", "")
                .Replace("Kuestenlogik.Surgewave.Storage.Tiering.", "")
                .Replace("Kuestenlogik.Surgewave.", "")),
            Version = version,
            Assemblies = [assemblyFileName]
        };
    }

    private static string GetDisplayName(string typeName)
    {
        var name = typeName
            .Replace("Connector", "")
            .Replace("Source", " Source")
            .Replace("Sink", " Sink");

        return System.Text.RegularExpressions.Regex.Replace(name, "(?<!^)([A-Z])", " $1").Trim();
    }

    private static async Task<PluginManifest?> TryReadManifestFromDirectoryAsync(string pluginDir, CancellationToken cancellationToken)
    {
        var manifestPath = Path.Combine(pluginDir, ManifestFileName);
        if (!File.Exists(manifestPath))
            return null;

        try
        {
            var json = await File.ReadAllTextAsync(manifestPath, cancellationToken);
            return JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Walks every plugin subdirectory under <paramref name="pluginsDir"/>, reads each
    /// <see cref="SurgewavePackageConventions.ManifestFileName"/>, and yields the absolute path
    /// of the bundled <c>pluginsettings.json</c> (or whatever filename the manifest's
    /// <see cref="PluginManifest.PluginSettings"/> field declares). Plugin authors can pick
    /// any filename — the broker and the <c>surgewave config validate</c> CLI both look it up
    /// via the manifest, so the convention is honoured but not enforced.
    ///
    /// <para>
    /// Plugins without a manifest, plugins without a bundled settings file, or unreadable
    /// manifests are silently skipped — discovery is best-effort, not validation. Use
    /// <c>surgewave config validate</c> for the validation pass.
    /// </para>
    /// </summary>
    public static IEnumerable<string> EnumerateInstalledPluginSettingsFiles(string pluginsDir)
    {
        if (!Directory.Exists(pluginsDir)) yield break;

        foreach (var pluginDir in Directory.EnumerateDirectories(pluginsDir).OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
        {
            var manifestPath = Path.Combine(pluginDir, ManifestFileName);
            if (!File.Exists(manifestPath)) continue;

            string? settingsFileName;
            try
            {
                var json = File.ReadAllText(manifestPath);
                var manifest = JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions);
                settingsFileName = string.IsNullOrWhiteSpace(manifest?.PluginSettings)
                    ? PluginSettingsFileName
                    : manifest.PluginSettings;
            }
            catch
            {
                continue;
            }

            var settingsPath = Path.Combine(pluginDir, settingsFileName);
            if (File.Exists(settingsPath))
            {
                yield return settingsPath;
            }
        }
    }

    private static string? GetTargetPath(string pluginDir, string entryPath, string pluginSettingsName)
    {
        string? candidate = null;

        if (entryPath.StartsWith($"{LibDirectory}/", StringComparison.OrdinalIgnoreCase))
        {
            var relativePath = entryPath.Substring(LibDirectory.Length + 1);
            candidate = Path.Combine(pluginDir, relativePath);
        }
        else if (entryPath.StartsWith($"{SharedDirectory}/", StringComparison.OrdinalIgnoreCase))
        {
            var relativePath = entryPath.Substring(SharedDirectory.Length + 1);
            candidate = Path.Combine(pluginDir, relativePath);
        }
        else if (entryPath.StartsWith($"{DepsDirectory}/", StringComparison.OrdinalIgnoreCase))
        {
            candidate = Path.Combine(pluginDir, entryPath);
        }
        else if (entryPath.Equals(ManifestFileName, StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals(pluginSettingsName, StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals(SbomGenerator.SbomFileName, StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals("README.md", StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals("LICENSE", StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals("icon.png", StringComparison.OrdinalIgnoreCase) ||
                 entryPath.Equals("icon.svg", StringComparison.OrdinalIgnoreCase))
        {
            candidate = Path.Combine(pluginDir, entryPath);
        }

        if (candidate is null)
        {
            return null;
        }

        // Zip-Slip-Schutz: Auch wenn entryPath mit einem der Allowlist-
        // Prefixes anfaengt (z.B. 'lib/'), koennte der Rest des Pfads
        // '../../..' enthalten und so aus pluginDir ausbrechen. Wir
        // resolven beide Pfade absolut und verlangen Containment.
        var pluginRoot = Path.GetFullPath(pluginDir);
        var resolved = Path.GetFullPath(candidate);
        if (!resolved.StartsWith(pluginRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            && !string.Equals(resolved, pluginRoot, StringComparison.Ordinal))
        {
            return null;
        }

        return resolved;
    }
}
