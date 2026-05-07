namespace Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A single configuration key with its type, default value, importance, and documentation.
/// </summary>
public sealed record ConfigKey(
    string Name,
    ConfigType Type,
    object? DefaultValue,
    Importance Importance,
    string Documentation,
    EditorHint Editor = EditorHint.Default,
    string? EditorLanguage = null,
    string[]? Options = null);
