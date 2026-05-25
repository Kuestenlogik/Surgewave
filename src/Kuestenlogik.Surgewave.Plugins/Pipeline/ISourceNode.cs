namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// A pipeline node that produces data from an external system.
/// Source nodes have no input ports (InputPorts=0) and at least one output port.
/// </summary>
public interface ISourceNode : IPipelineNode;
