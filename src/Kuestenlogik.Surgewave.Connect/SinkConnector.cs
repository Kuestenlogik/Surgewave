namespace Kuestenlogik.Surgewave.Connect;

using Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Base class for sink connectors that consume records from Surgewave topics and write them to external systems.
/// Sink nodes have at least one input port and no output ports.
/// </summary>
public abstract class SinkConnector : Connector, ISinkNode
{
    /// <summary>Sink nodes have one input port.</summary>
    public override int InputPorts => 1;

    /// <summary>Sink nodes have no output ports.</summary>
    public override int OutputPorts => 0;
}
