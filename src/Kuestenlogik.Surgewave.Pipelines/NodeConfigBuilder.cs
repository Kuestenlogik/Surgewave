namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Fluent configuration for a generic pipeline node added via the <c>Through</c> escape hatch.
/// Keys are connector config keys as declared by the node's <c>ConfigDef</c>.
/// </summary>
public sealed class NodeConfigBuilder
{
    private readonly Dictionary<string, string> _config = [];

    /// <summary>Sets a config value.</summary>
    public NodeConfigBuilder Set(string key, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        _config[key] = value;
        return this;
    }

    /// <summary>Sets a config value from any formattable value using invariant formatting.</summary>
    public NodeConfigBuilder Set(string key, IFormattable value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Set(key, value.ToString(null, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Sets a boolean config value as <c>true</c>/<c>false</c>.</summary>
    public NodeConfigBuilder Set(string key, bool value)
    {
        return Set(key, value ? "true" : "false");
    }

    internal IReadOnlyDictionary<string, string> Build()
    {
        return _config;
    }
}
