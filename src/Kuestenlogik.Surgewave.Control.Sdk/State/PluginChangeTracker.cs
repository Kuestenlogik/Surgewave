namespace Kuestenlogik.Surgewave.Control.State;

/// <summary>
/// Per-circuit tracker for plugin install / uninstall / update actions that
/// require a broker restart to fully take effect. The Plugins pages call
/// <see cref="MarkChanged"/> after a successful action; <c>PluginRestartBanner</c>
/// subscribes to <see cref="StateChanged"/> and shows a persistent banner with
/// the list of affected packages until the operator clicks "Acknowledge"
/// (after restarting the broker). Scoped to the Blazor circuit so two browser
/// tabs don't share the banner state.
/// </summary>
public sealed class PluginChangeTracker
{
    private readonly HashSet<string> _changed = new(StringComparer.OrdinalIgnoreCase);

    public event Action? StateChanged;

    public bool PendingRestart => _changed.Count > 0;

    public IReadOnlyCollection<string> ChangedPackages => _changed;

    public void MarkChanged(string packageId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(packageId);
        if (_changed.Add(packageId))
        {
            StateChanged?.Invoke();
        }
    }

    /// <summary>
    /// Clear all tracked changes — operator has acknowledged the restart.
    /// </summary>
    public void Acknowledge()
    {
        if (_changed.Count == 0) return;
        _changed.Clear();
        StateChanged?.Invoke();
    }
}
