namespace Kuestenlogik.Surgewave.Plugins.Marketplace;

/// <summary>
/// Wizard-facing bucket a marketplace entry falls into. Derived from
/// the package's NuGet tags (lowercase, hyphenated by convention).
/// Operators see these as the section headers in the setup wizard
/// (*"Storage engines"*, *"Connectors"*, etc.) so adding a new
/// category is a UI-visible decision — change carefully.
/// </summary>
public enum PluginCategory
{
    /// <summary>Tag includes <c>storage-engine</c>, <c>storage</c>, or <c>tiered-storage</c>.</summary>
    StorageEngine,

    /// <summary>Tag includes <c>connector</c>, <c>source</c>, or <c>sink</c>.</summary>
    Connector,

    /// <summary>Tag includes <c>protocol</c> (Kafka-wire, MQTT, AMQP, …).</summary>
    Protocol,

    /// <summary>Tag includes <c>broker-extension</c> or <c>broker-plugin</c>.</summary>
    BrokerExtension,

    /// <summary>Tag includes <c>schema-handler</c> or <c>schema-format</c>.</summary>
    SchemaHandler,

    /// <summary>Tag includes <c>ai</c>, <c>llm</c>, or <c>embedding</c>.</summary>
    Ai,

    /// <summary>Surgewave-tagged but does not match any known bucket.</summary>
    Other,
}

/// <summary>
/// Maps a package's tag list onto a single <see cref="PluginCategory"/>.
/// Stable mapping rules; rule changes are user-visible (wizard renders
/// the bucket as a section header) so each addition is an explicit
/// step.
/// </summary>
public static class PluginCategoryClassifier
{
    public static PluginCategory Classify(IReadOnlyList<string>? tags)
    {
        if (tags is null || tags.Count == 0) return PluginCategory.Other;
        var normalised = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var t in tags) normalised.Add(t.Trim());

        if (Any(normalised, "storage-engine", "storage", "tiered-storage")) return PluginCategory.StorageEngine;
        if (Any(normalised, "connector", "source", "sink")) return PluginCategory.Connector;
        if (Any(normalised, "protocol")) return PluginCategory.Protocol;
        if (Any(normalised, "schema-handler", "schema-format")) return PluginCategory.SchemaHandler;
        if (Any(normalised, "ai", "llm", "embedding")) return PluginCategory.Ai;
        if (Any(normalised, "broker-extension", "broker-plugin")) return PluginCategory.BrokerExtension;
        return PluginCategory.Other;
    }

    private static bool Any(HashSet<string> haystack, params string[] needles)
    {
        foreach (var n in needles)
        {
            if (haystack.Contains(n)) return true;
        }
        return false;
    }
}
