namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// A pipeline definition packaged in a library. Classes implementing this interface are
/// discovered by <c>surgewave pipelines deploy MyPipelines.dll</c> — build the assembly,
/// deploy the pipelines, no broker reference required.
/// An unnamed pipeline is deployed under the implementing class's name in kebab-case.
/// </summary>
public interface ISurgewavePipeline
{
    /// <summary>Defines and builds the pipeline.</summary>
    BuiltPipeline Define();
}
