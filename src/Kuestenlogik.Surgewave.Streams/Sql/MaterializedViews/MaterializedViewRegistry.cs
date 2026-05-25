using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Process-wide registry of materialized views.
/// Thread-safe; intended to be a DI singleton.
///
/// The registry only tracks views — refresh and state maintenance live in
/// <c>MaterializedViewRefreshService</c>.
/// </summary>
public sealed class MaterializedViewRegistry
{
    private readonly ConcurrentDictionary<string, MaterializedView> _views =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a new view. Returns false when a view with the same name
    /// already exists and <c>definition.IfNotExists</c> is false.
    /// When <c>definition.IfNotExists</c> is true, an existing view is left
    /// untouched and the call still returns true.
    /// </summary>
    public bool TryRegister(ViewDefinition definition, out MaterializedView view)
    {
        view = new MaterializedView(definition);
        if (_views.TryAdd(definition.Name, view))
            return true;

        if (definition.IfNotExists && _views.TryGetValue(definition.Name, out var existing))
        {
            view = existing;
            return true;
        }

        return false;
    }

    /// <summary>Removes a view. Returns false if no such view exists.</summary>
    public bool TryUnregister(string name, out MaterializedView removed)
        => _views.TryRemove(name, out removed!);

    public bool TryGet(string name, out MaterializedView view)
        => _views.TryGetValue(name, out view!);

    public bool Contains(string name) => _views.ContainsKey(name);

    public IReadOnlyCollection<MaterializedView> All => _views.Values.ToArray();

    public IReadOnlyCollection<string> Names => _views.Keys.ToArray();

    public int Count => _views.Count;
}
