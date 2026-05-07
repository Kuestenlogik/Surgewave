namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// A pipeline node that writes data to an external system.
/// Sink nodes have at least one input port and no output ports (OutputPorts=0).
/// </summary>
public interface ISinkNode : IPipelineNode;
