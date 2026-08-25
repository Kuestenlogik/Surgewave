using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Cli.Commands.Config;

/// <summary>
/// Shared helpers for loading, merging and navigating Surgewave configuration JSON trees.
/// Used by <see cref="ConfigValidateCommand"/> and <see cref="ConfigViewCommand"/> so
/// the file-loading, plugin-defaults-layering and JSON-overlay logic lives in one place.
/// </summary>
internal static class ConfigLoader
{
    /// <summary>
    /// Reads a JSON file and returns it as a <see cref="JsonObject"/>.
    /// Returns <c>null</c> if the top-level value is not an object.
    /// Supports JSON5-ish files (comments + trailing commas — the same leniency
    /// that <c>Microsoft.Extensions.Configuration.Json</c> applies to appsettings).
    /// </summary>
    public static JsonObject? LoadJsonObject(string path)
    {
        using var stream = File.OpenRead(path);
        var node = JsonNode.Parse(stream, documentOptions: new JsonDocumentOptions
        {
            CommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
        });
        return node as JsonObject;
    }

    /// <summary>
    /// Resolves the plugins directory for a config file being inspected.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deliberately deployment-relative rather than machine-relative: these commands
    /// answer "what does this appsettings.json mean", possibly for an installation
    /// on another host, so the plugins next to that file are the right ones. That is
    /// a different question from "what would a broker load here", which
    /// <see cref="SurgewavePluginDirectories"/> answers.
    /// </para>
    /// <para>
    /// The comment here used to claim this was the broker's own heuristic. It was
    /// not even then — the broker read <c>plugins</c> beside its executable — and
    /// with the scopes of #158 it is further from it still. Only the last resort
    /// changed: falling back to the CLI's own directory described nothing at all,
    /// so an unaccompanied config file now falls back to the machine's scopes.
    /// </para>
    /// </remarks>
    public static string ResolvePluginsDirectory(string configFullPath, string? assembliesDir)
    {
        if (!string.IsNullOrEmpty(assembliesDir))
            return Path.Combine(Path.GetFullPath(assembliesDir), "plugins");

        var configDir = Path.GetDirectoryName(configFullPath);
        if (!string.IsNullOrEmpty(configDir))
        {
            var beside = Path.Combine(configDir, "plugins");
            if (Directory.Exists(beside)) return beside;
        }

        return SurgewavePluginDirectories.SearchOrder()[^1];
    }

    /// <summary>
    /// Lists every installed plugin's bundled settings file path via
    /// <see cref="PluginPackageManager.EnumerateInstalledPluginSettingsFiles"/>.
    /// </summary>
    public static List<string> DiscoverPluginSettingsFiles(string pluginsDir)
        => PluginPackageManager.EnumerateInstalledPluginSettingsFiles(pluginsDir).ToList();

    /// <summary>
    /// Loads the user config from <paramref name="configFullPath"/>, layers in plugin
    /// defaults from <paramref name="pluginsDir"/> beneath it, and returns the merged
    /// <see cref="JsonObject"/>. User values always win; plugin defaults fill the gaps.
    ///
    /// <para>
    /// Optionally populates <paramref name="sources"/> with per-leaf source attribution
    /// (key = colon-separated config path, value = source label). Pass <c>null</c> to
    /// skip tracking.
    /// </para>
    /// </summary>
    public static (JsonObject? merged, int pluginCount) LoadAndMerge(
        string configFullPath,
        string pluginsDir,
        Dictionary<string, string>? sources = null)
    {
        var pluginFiles = DiscoverPluginSettingsFiles(pluginsDir);
        var rootObject = new JsonObject();

        // Layer 1 (lowest priority): plugin defaults
        foreach (var pluginFile in pluginFiles)
        {
            JsonObject? pluginObj;
            try { pluginObj = LoadJsonObject(pluginFile); }
            catch { continue; }
            if (pluginObj is null) continue;

            if (sources is not null)
            {
                var sourceLabel = MakeRelativeLabel(pluginFile, pluginsDir);
                OverlayJsonObject(rootObject, pluginObj, sources, prefix: "", sourceLabel);
            }
            else
            {
                OverlayJsonObject(rootObject, pluginObj);
            }
        }

        // Layer 2 (highest priority): user config
        var userObject = LoadJsonObject(configFullPath);
        if (userObject is null) return (null, pluginFiles.Count);

        if (sources is not null)
        {
            var userLabel = Path.GetFileName(configFullPath);
            OverlayJsonObject(rootObject, userObject, sources, prefix: "", userLabel);
        }
        else
        {
            OverlayJsonObject(rootObject, userObject);
        }

        return (rootObject, pluginFiles.Count);
    }

    /// <summary>
    /// Walks the JSON tree along a colon-separated path (e.g. "Surgewave:Mqtt:Port") and
    /// returns the node at the end, or <c>null</c> if any hop is missing.
    /// </summary>
    public static JsonNode? NavigateSection(JsonNode root, string sectionName)
    {
        var current = root;
        foreach (var segment in sectionName.Split(':'))
        {
            if (current is not JsonObject obj) return null;
            if (!obj.TryGetPropertyValue(segment, out var next) || next is null) return null;
            current = next;
        }
        return current;
    }

    /// <summary>
    /// Simple overlay without source tracking. Overlay values replace base values;
    /// when both sides are objects, recurse so base keys survive unless individually
    /// overridden.
    /// </summary>
    public static void OverlayJsonObject(JsonObject baseObj, JsonObject overlay)
    {
        foreach (var (key, overlayValue) in overlay)
        {
            if (overlayValue is JsonObject overlayChild &&
                baseObj.TryGetPropertyValue(key, out var baseChild) &&
                baseChild is JsonObject baseChildObj)
            {
                OverlayJsonObject(baseChildObj, overlayChild);
            }
            else
            {
                baseObj[key] = overlayValue?.DeepClone();
            }
        }
    }

    /// <summary>
    /// Overlay with per-leaf source tracking. Same merge semantics as the simple
    /// overload, but every leaf write is recorded in <paramref name="sources"/>
    /// keyed by the colon-separated config path (e.g. "Surgewave:Mqtt:Port").
    /// </summary>
    public static void OverlayJsonObject(
        JsonObject baseObj,
        JsonObject overlay,
        Dictionary<string, string> sources,
        string prefix,
        string sourceLabel)
    {
        foreach (var (key, overlayValue) in overlay)
        {
            var path = string.IsNullOrEmpty(prefix) ? key : $"{prefix}:{key}";

            if (overlayValue is JsonObject overlayChild &&
                baseObj.TryGetPropertyValue(key, out var baseChild) &&
                baseChild is JsonObject baseChildObj)
            {
                OverlayJsonObject(baseChildObj, overlayChild, sources, path, sourceLabel);
            }
            else
            {
                baseObj[key] = overlayValue?.DeepClone();
                if (overlayValue is JsonObject newChildObj)
                {
                    RecordSourcesForSubtree(newChildObj, sources, path, sourceLabel);
                }
                else
                {
                    sources[path] = sourceLabel;
                }
            }
        }
    }

    /// <summary>
    /// Turns an absolute path into a short relative label (e.g.
    /// "plugins/kuestenlogik.surgewave.protocol.mqtt/pluginsettings.json") for source attribution.
    /// </summary>
    public static string MakeRelativeLabel(string fullPath, string pluginsDir)
    {
        var anchorParent = Path.GetDirectoryName(pluginsDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        if (string.IsNullOrEmpty(anchorParent)) return fullPath;
        if (fullPath.StartsWith(anchorParent, StringComparison.OrdinalIgnoreCase))
        {
            return fullPath.Substring(anchorParent.Length)
                .TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Replace('\\', '/');
        }
        return fullPath;
    }

    private static void RecordSourcesForSubtree(
        JsonObject obj,
        Dictionary<string, string> sources,
        string prefix,
        string sourceLabel)
    {
        foreach (var (key, value) in obj)
        {
            var path = $"{prefix}:{key}";
            if (value is JsonObject child)
            {
                RecordSourcesForSubtree(child, sources, path, sourceLabel);
            }
            else
            {
                sources[path] = sourceLabel;
            }
        }
    }
}
