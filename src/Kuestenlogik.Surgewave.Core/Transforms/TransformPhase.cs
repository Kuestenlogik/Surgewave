namespace Kuestenlogik.Surgewave.Core.Transforms;

/// <summary>
/// Indicates at which phase of the data path a transform executes.
/// </summary>
public enum TransformPhase
{
    /// <summary>
    /// Transform runs during the produce (write) path before data is stored.
    /// </summary>
    Produce,

    /// <summary>
    /// Transform runs during the fetch (read) path before data is returned to the consumer.
    /// </summary>
    Fetch
}
