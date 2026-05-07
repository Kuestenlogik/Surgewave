using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.Build.Framework;
using MsBuildTask = Microsoft.Build.Utilities.Task;

namespace Kuestenlogik.Surgewave.Build.Tasks;

/// <summary>
/// MSBuild task that installs a <c>.swpkg</c> archive into a plugins directory.
/// Delegates to <see cref="PluginPackageManager"/> — same logic as <c>surgewave plugins install</c>.
/// No broker connection required; operates on the file system only.
/// </summary>
public sealed class InstallPluginTask : MsBuildTask
{
    /// <summary>Full path to the <c>.swpkg</c> file to install.</summary>
    [Required]
    public string PackagePath { get; set; } = "";

    /// <summary>Target plugins directory (e.g. the broker's <c>plugins/</c> folder).</summary>
    [Required]
    public string PluginsDir { get; set; } = "";

    /// <summary>Overwrite an existing installation of the same plugin.</summary>
    public bool Force { get; set; }

    public override bool Execute()
    {
        var manager = new PluginPackageManager();
        try
        {
            var result = manager
                .InstallAsync(PackagePath, PluginsDir, Force, cancellationToken: CancellationToken.None)
                .GetAwaiter().GetResult();

            if (!result.Success)
            {
                Log.LogError($"Surgewave: plugin install failed: {result.Error}");
                return false;
            }

            Log.LogMessage(MessageImportance.High,
                $"Surgewave: installed {result.Manifest?.Name} v{result.Manifest?.Version} → {PluginsDir}");
            return true;
        }
        catch (Exception ex)
        {
            Log.LogError($"Surgewave: plugin install failed: {ex.Message}");
            return false;
        }
    }
}
