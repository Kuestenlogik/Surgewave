using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Central registry of <see cref="IPeerTransport"/> implementations. Each
/// transport assembly (TCP, QUIC, ...) auto-registers here via a module
/// initializer, so merely referencing the assembly is enough to make the
/// transport selectable by name in configuration.
/// </summary>
public static class PeerTransportFactory
{
    private static readonly ConcurrentDictionary<string, Func<IPeerTransport>> _transports =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a peer transport factory under the given short name
    /// (e.g. "tcp", "quic"). Subsequent <see cref="Create"/> calls resolve
    /// by name. Idempotent — re-registering the same name replaces the entry.
    /// </summary>
    public static void Register(string name, Func<IPeerTransport> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(factory);
        _transports[name] = factory;
    }

    /// <summary>
    /// Creates an instance of the named peer transport. Throws if the
    /// transport assembly has not been referenced (and therefore not
    /// registered via its module initializer).
    /// </summary>
    public static IPeerTransport Create(string name)
    {
        if (_transports.TryGetValue(name, out var factory))
        {
            return factory();
        }

        throw new InvalidOperationException(
            $"Peer transport '{name}' is not registered. Add a reference to the corresponding transport assembly "
            + "(Kuestenlogik.Surgewave.Transport.Tcp, Kuestenlogik.Surgewave.Transport.Quic, ...) so its module initializer runs.");
    }

    /// <summary>Names of all currently registered peer transports.</summary>
    public static IReadOnlyCollection<string> RegisteredNames => (IReadOnlyCollection<string>)_transports.Keys;

    /// <summary>
    /// Checks whether a transport with the given name is currently registered.
    /// </summary>
    public static bool IsRegistered(string name) =>
        !string.IsNullOrWhiteSpace(name) && _transports.ContainsKey(name);

    /// <summary>
    /// Tries to create the requested transport, falling back to
    /// <paramref name="fallbackName"/> if the primary is unregistered or if
    /// its factory throws a <see cref="PlatformNotSupportedException"/>.
    /// Returns the created transport and sets <paramref name="fellBack"/>
    /// when the fallback was used. Designed for startup-time resolution
    /// where a missing feature (e.g. msquic) should degrade rather than crash.
    /// </summary>
    public static IPeerTransport CreateWithFallback(
        string requestedName,
        string fallbackName,
        out bool fellBack)
    {
        fellBack = false;

        if (IsRegistered(requestedName))
        {
            try
            {
                return Create(requestedName);
            }
            catch (PlatformNotSupportedException)
            {
                // Transport registered but runtime prerequisites missing — fall through.
            }
        }

        fellBack = true;
        return Create(fallbackName);
    }
}
