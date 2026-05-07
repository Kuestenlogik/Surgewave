using System.ComponentModel.DataAnnotations;

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Request to create a new connector.
/// </summary>
public sealed class CreateConnectorRequest
{
    /// <summary>
    /// Name of the connector.
    /// </summary>
    [Required]
    public string? Name { get; set; }

    /// <summary>
    /// Connector configuration including connector.class.
    /// </summary>
    [Required]
    public Dictionary<string, string>? Config { get; set; }
}
