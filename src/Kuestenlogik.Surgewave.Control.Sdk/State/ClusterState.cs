namespace Kuestenlogik.Surgewave.Control.State;

public class ClusterState
{
    private string? _selectedClusterId;

    public string? SelectedClusterId
    {
        get => _selectedClusterId;
        set
        {
            if (_selectedClusterId != value)
            {
                _selectedClusterId = value;
                OnChange?.Invoke();
            }
        }
    }

    public event Action? OnChange;
}
