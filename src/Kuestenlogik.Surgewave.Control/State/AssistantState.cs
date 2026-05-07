using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.State;

/// <summary>
/// Scoped state service tracking assistant drawer visibility, conversation history, and detected anomalies.
/// One instance per Blazor circuit.
/// </summary>
public sealed class AssistantState
{
    private readonly List<AssistantMessage> _messages = [];
    private readonly List<AnomalyDetection> _latestAnomalies = [];
    private bool _isOpen;
    private bool _isProcessing;

    /// <summary>Whether the assistant drawer is currently open.</summary>
    public bool IsOpen
    {
        get => _isOpen;
        set
        {
            if (_isOpen != value)
            {
                _isOpen = value;
                NotifyStateChanged();
            }
        }
    }

    /// <summary>Whether a request is currently being processed.</summary>
    public bool IsProcessing
    {
        get => _isProcessing;
        set
        {
            if (_isProcessing != value)
            {
                _isProcessing = value;
                NotifyStateChanged();
            }
        }
    }

    /// <summary>The current conversation messages.</summary>
    public IReadOnlyList<AssistantMessage> Messages => _messages;

    /// <summary>The most recently detected anomalies.</summary>
    public IReadOnlyList<AnomalyDetection> LatestAnomalies => _latestAnomalies;

    /// <summary>Current assistant settings.</summary>
    public AssistantSettings Settings { get; } = new();

    /// <summary>Fired when any state property changes.</summary>
    public event Action? OnChange;

    /// <summary>Toggles the drawer open/closed.</summary>
    public void ToggleDrawer()
    {
        _isOpen = !_isOpen;
        NotifyStateChanged();
    }

    /// <summary>Adds a message to the conversation history.</summary>
    public void AddMessage(AssistantMessage message)
    {
        _messages.Add(message);
        NotifyStateChanged();
    }

    /// <summary>Replaces the latest anomalies list.</summary>
    public void UpdateAnomalies(IEnumerable<AnomalyDetection> anomalies)
    {
        _latestAnomalies.Clear();
        _latestAnomalies.AddRange(anomalies);
        NotifyStateChanged();
    }

    /// <summary>Clears all conversation history.</summary>
    public void ClearConversation()
    {
        _messages.Clear();
        NotifyStateChanged();
    }

    /// <summary>Returns the number of unread anomalies (critical + warning).</summary>
    public int UnreadAlertCount => _latestAnomalies.Count(a => a.Severity is "Critical" or "Warning");

    private void NotifyStateChanged() => OnChange?.Invoke();
}
