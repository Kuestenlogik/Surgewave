namespace Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// Configuration definition for plugins and pipeline nodes.
/// </summary>
public sealed class ConfigDef
{
    private readonly List<ConfigKey> _keys = [];

    public IReadOnlyList<ConfigKey> Keys => _keys;

    public ConfigDef Define(string name, ConfigType type, object? defaultValue, Importance importance, string documentation)
    {
        _keys.Add(new ConfigKey(name, type, defaultValue, importance, documentation));
        return this;
    }

    public ConfigDef Define(string name, ConfigType type, Importance importance, string documentation)
    {
        return Define(name, type, null, importance, documentation);
    }

    public ConfigDef Define(string name, ConfigType type, object? defaultValue, Importance importance,
        string documentation, EditorHint editor, string? editorLanguage = null, string[]? options = null)
    {
        _keys.Add(new ConfigKey(name, type, defaultValue, importance, documentation, editor, editorLanguage, options));
        return this;
    }

    public ConfigDef Define(string name, ConfigType type, Importance importance,
        string documentation, EditorHint editor, string? editorLanguage = null, string[]? options = null)
    {
        return Define(name, type, null, importance, documentation, editor, editorLanguage, options);
    }
}
