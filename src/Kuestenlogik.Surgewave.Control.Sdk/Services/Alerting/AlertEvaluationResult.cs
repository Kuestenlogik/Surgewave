namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Outcome of evaluating one alert rule against the current broker state.
/// </summary>
public readonly record struct AlertEvaluationResult(bool Triggered, double CurrentValue, string Message)
{
    public static readonly AlertEvaluationResult NotTriggered = new(false, 0, "");
}
