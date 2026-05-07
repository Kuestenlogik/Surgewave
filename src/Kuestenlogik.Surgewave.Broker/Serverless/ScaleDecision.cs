namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Result of a scaling evaluation, containing the recommended action,
/// human-readable reason, and target broker count.
/// </summary>
public sealed record ScaleDecision(ScaleAction Action, string Reason, int TargetBrokerCount);
