namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Thrown when a pipeline definition under construction is invalid — an unnamed pipeline is
/// exported, a connection references an unknown node, the graph contains a cycle, or a
/// fluent stage is used in a way the target node cannot express.
/// </summary>
public sealed class PipelineBuildException : Exception
{
    public PipelineBuildException(string message)
        : base(message)
    {
    }

    public PipelineBuildException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
