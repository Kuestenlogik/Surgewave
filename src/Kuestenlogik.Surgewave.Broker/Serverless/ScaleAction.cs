namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Possible scaling actions returned by the scale decision engine.
/// </summary>
public enum ScaleAction
{
    /// <summary>No scaling needed; cluster is appropriately sized.</summary>
    NoChange,
    /// <summary>Add one or more broker instances.</summary>
    ScaleUp,
    /// <summary>Remove one or more broker instances.</summary>
    ScaleDown
}
