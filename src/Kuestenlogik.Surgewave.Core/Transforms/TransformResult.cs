namespace Kuestenlogik.Surgewave.Core.Transforms;

/// <summary>
/// Result of an inline transform execution. Supports pass-through, drop (filter),
/// value mutation, and topic routing.
/// </summary>
public sealed class TransformResult
{
    /// <summary>
    /// When true the record is dropped (filtered out) and will not be stored or returned.
    /// </summary>
    public bool Dropped { get; private init; }

    /// <summary>
    /// The (possibly transformed) record key.
    /// </summary>
    public byte[] Key { get; private init; } = [];

    /// <summary>
    /// The (possibly transformed) record value.
    /// </summary>
    public byte[] Value { get; private init; } = [];

    /// <summary>
    /// Optional transformed headers. Null means keep original headers.
    /// </summary>
    public Dictionary<string, byte[]>? Headers { get; private init; }

    /// <summary>
    /// When non-null the record is rerouted to the specified topic instead of the original.
    /// </summary>
    public string? RouteTopic { get; private init; }

    /// <summary>
    /// Creates a pass-through result with the given key and value (possibly mutated).
    /// </summary>
    public static TransformResult Pass(byte[] key, byte[] value, Dictionary<string, byte[]>? headers = null)
        => new() { Key = key, Value = value, Headers = headers };

    /// <summary>
    /// Creates a result that drops (filters out) the record.
    /// </summary>
    public static TransformResult Drop()
        => new() { Dropped = true };

    /// <summary>
    /// Creates a result that reroutes the record to a different topic.
    /// </summary>
    public static TransformResult Route(string topic, byte[] key, byte[] value, Dictionary<string, byte[]>? headers = null)
        => new() { Key = key, Value = value, Headers = headers, RouteTopic = topic };
}
