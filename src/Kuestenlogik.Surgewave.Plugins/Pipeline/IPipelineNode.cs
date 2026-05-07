using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Universal pipeline building block. Every node in a Surgewave pipeline
/// implements this interface. Nodes are connected via ports — a node
/// with InputPorts=0 is a start node, OutputPorts=0 is an end node.
/// </summary>
public interface IPipelineNode : IPlugin
{
    /// <summary>
    /// Number of input ports (0 = start/source node).
    /// </summary>
    int InputPorts { get; }

    /// <summary>
    /// Number of output ports (0 = end/sink node).
    /// </summary>
    int OutputPorts { get; }

    /// <summary>
    /// Configuration schema for this node.
    /// </summary>
    ConfigDef Config { get; }

    /// <summary>
    /// Semantic version of this node implementation.
    /// </summary>
    string Version { get; }
}
