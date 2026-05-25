using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.Build.Framework;
using MsBuildTask = Microsoft.Build.Utilities.Task;

namespace Kuestenlogik.Surgewave.Build.Tasks;

/// <summary>
/// MSBuild task that packs a plugin's publish output into a <c>.swpkg</c> archive.
/// Delegates to <see cref="PluginPackageManager"/> — same logic as <c>surgewave plugins pack</c>.
/// </summary>
public sealed class PackPluginTask : MsBuildTask
{
    /// <summary>Directory containing the plugin's published output (with plugin.json manifest).</summary>
    [Required]
    public string SourceDir { get; set; } = "";

    /// <summary>Directory where the <c>.swpkg</c> file will be written.</summary>
    [Required]
    public string OutputDir { get; set; } = "";

    /// <summary>Optional path to a custom <c>plugin.json</c> manifest (default: auto-detected in SourceDir).</summary>
    public string? ManifestPath { get; set; }

    /// <summary>Optional path to ECDSA private key for signing the package.</summary>
    public string? SigningKeyPath { get; set; }

    /// <summary>Full path to the created <c>.swpkg</c> file (output property).</summary>
    [Output]
    public string? PackagePath { get; set; }

    public override bool Execute()
    {
        var manager = new PluginPackageManager();
        try
        {
            ISppSigner? signer = !string.IsNullOrEmpty(SigningKeyPath) && File.Exists(SigningKeyPath)
                ? new BuiltinEcdsaSigner(privateKeyPath: SigningKeyPath)
                : null;

            PackagePath = manager
                .PackAsync(SourceDir, string.IsNullOrEmpty(ManifestPath) ? null : ManifestPath, OutputDir,
                    signer: signer)
                .GetAwaiter().GetResult();

            Log.LogMessage(MessageImportance.High, $"Surgewave: packed plugin → {PackagePath}");
            return true;
        }
        catch (Exception ex)
        {
            Log.LogError($"Surgewave: plugin pack failed: {ex.Message}");
            return false;
        }
    }
}
