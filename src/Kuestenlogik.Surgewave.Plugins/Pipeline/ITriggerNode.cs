namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// A pipeline node that triggers on events (cron, webhook, topic arrival).
/// Trigger nodes have no input ports (InputPorts=0) and at least one output port.
/// Unlike source nodes, triggers are event-driven rather than polling-based.
/// </summary>
public interface ITriggerNode : IPipelineNode;
