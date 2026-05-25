namespace Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// Hints for the UI editor on how to render a configuration field.
/// </summary>
public enum EditorHint
{
    /// <summary>Standard text input.</summary>
    Default,

    /// <summary>Code editor with syntax highlighting (set EditorLanguage).</summary>
    Code,

    /// <summary>Multi-line text area.</summary>
    Multiline,

    /// <summary>Cron expression editor with preview.</summary>
    Cron,

    /// <summary>Expression editor (e.g., JSONPath, JMESPath).</summary>
    Expression,

    /// <summary>Condition editor (boolean expression).</summary>
    Condition,

    /// <summary>Topic name picker with auto-complete.</summary>
    Topic,

    /// <summary>Dropdown select from Options list.</summary>
    Select,

    /// <summary>File path picker.</summary>
    FilePath,

    /// <summary>SQL query editor.</summary>
    Sql
}
