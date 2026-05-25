using System.Collections.Frozen;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Dispatches native protocol requests to appropriate handlers using O(1) frozen dictionary lookup.
/// </summary>
public sealed class NativeRequestDispatcher
{
    private readonly FrozenDictionary<SurgewaveOpCode, INativeRequestHandler> _handlers;

    public NativeRequestDispatcher(IEnumerable<INativeRequestHandler> handlers)
    {
        var dict = new Dictionary<SurgewaveOpCode, INativeRequestHandler>();

        foreach (var handler in handlers)
        {
            foreach (var opCode in handler.SupportedOpCodes)
            {
                if (dict.ContainsKey(opCode))
                {
                    throw new InvalidOperationException(
                        $"Duplicate handler registration for native OpCode {opCode}");
                }
                dict[opCode] = handler;
            }
        }

        _handlers = dict.ToFrozenDictionary();
    }

    /// <summary>
    /// Check if a handler is registered for the given opcode.
    /// </summary>
    public bool HasHandler(SurgewaveOpCode opCode) => _handlers.ContainsKey(opCode);

    /// <summary>
    /// Get the handler for the given opcode, or null if none registered.
    /// </summary>
    public INativeRequestHandler? GetHandler(SurgewaveOpCode opCode)
    {
        return _handlers.TryGetValue(opCode, out var handler) ? handler : null;
    }

    /// <summary>
    /// Dispatch a request to the appropriate handler.
    /// Returns false if no handler is registered for the opcode.
    /// </summary>
    public async Task<bool> TryDispatchAsync(
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (_handlers.TryGetValue(context.Header.OpCode, out var handler))
        {
            await handler.HandleAsync(context, payload, cancellationToken);
            return true;
        }
        return false;
    }
}
