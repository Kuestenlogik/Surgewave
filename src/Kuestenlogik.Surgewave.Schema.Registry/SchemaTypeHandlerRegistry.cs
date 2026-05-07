using System.Collections.Frozen;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Default implementation of ISchemaTypeHandlerRegistry that collects handlers from DI.
/// </summary>
public sealed class SchemaTypeHandlerRegistry : ISchemaTypeHandlerRegistry
{
    private readonly FrozenDictionary<string, ISchemaTypeHandler> _handlers;

    public SchemaTypeHandlerRegistry(IEnumerable<ISchemaTypeHandler> handlers)
    {
        _handlers = handlers.ToFrozenDictionary(h => h.TypeName.ToUpperInvariant(), h => h, StringComparer.OrdinalIgnoreCase);
    }

    public IEnumerable<string> GetSupportedTypes() => _handlers.Keys;

    public ISchemaTypeHandler? GetHandler(string typeName)
    {
        return _handlers.TryGetValue(typeName.ToUpperInvariant(), out var handler) ? handler : null;
    }

    public bool IsSupported(string typeName)
    {
        return _handlers.ContainsKey(typeName.ToUpperInvariant());
    }
}
