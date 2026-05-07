namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Type of pipeline connection between nodes.
/// </summary>
public enum PipelineConnectionType
{
    /// <summary>Normal data flow connection.</summary>
    Normal,

    /// <summary>Error output connection for routing failed records to DLQ.</summary>
    Error
}
