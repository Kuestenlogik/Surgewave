namespace Kuestenlogik.Surgewave.Connect;

using Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Base class for source connectors that read data from external systems and produce records to Surgewave topics.
/// Source nodes have no input ports and at least one output port.
/// </summary>
public abstract class SourceConnector : Connector, ISourceNode
{
    /// <summary>Source nodes have no input ports.</summary>
    public override int InputPorts => 0;

    /// <summary>Source nodes have one output port.</summary>
    public override int OutputPorts => 1;
}
