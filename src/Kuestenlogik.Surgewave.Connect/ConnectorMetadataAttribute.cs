namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Attribute to provide metadata about a connector.
/// Applied to connector classes to enable discovery of display name, description, etc.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ConnectorMetadataAttribute : Attribute
{
    /// <summary>
    /// The display name of the connector.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// A brief description of what the connector does.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// The version of the connector (overrides the Version property if set).
    /// </summary>
    public string? Version { get; init; }

    /// <summary>
    /// The author or organization that created the connector.
    /// </summary>
    public string? Author { get; init; }

    /// <summary>
    /// A URL for documentation or the project homepage.
    /// </summary>
    public string? DocumentationUrl { get; init; }

    /// <summary>
    /// A URL for the license.
    /// </summary>
    public string? LicenseUrl { get; init; }

    /// <summary>
    /// Tags/keywords for categorization and search.
    /// Comma-separated list.
    /// </summary>
    public string? Tags { get; init; }

    /// <summary>
    /// Icon name (e.g., MudBlazor icon like "FileDocument") or resource path for embedded SVG.
    /// For embedded icons, use format: "resource:Namespace.Icons.MyIcon.svg"
    /// </summary>
    public string? Icon { get; init; }
}
