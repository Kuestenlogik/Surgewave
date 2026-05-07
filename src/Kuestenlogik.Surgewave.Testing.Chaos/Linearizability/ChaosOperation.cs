namespace Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;

/// <summary>
/// One event in a <see cref="History"/>. A Jepsen-style recording splits each client
/// call into <b>invoke</b> (call entered) and <b>ok</b> / <b>fail</b> (call returned),
/// so the checker can reason about requests whose outcome is unknown because of
/// a mid-call fault (timeout, broker crash). The <see cref="ClientId"/> disambiguates
/// concurrent calls from different clients in a multi-threaded history.
/// </summary>
public abstract record ChaosOperation
{
    /// <summary>Logical client id — each producer/consumer thread uses its own.</summary>
    public required int ClientId { get; init; }

    /// <summary>Monotonically increasing timestamp at the moment the operation was recorded.</summary>
    public required DateTimeOffset Timestamp { get; init; }
}

/// <summary>A producer began sending a record and is awaiting acknowledgement.</summary>
public sealed record ProduceInvoke : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required byte[] Value { get; init; }
}

/// <summary>A previously-invoked produce was acknowledged with a committed offset.</summary>
public sealed record ProduceOk : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required byte[] Value { get; init; }
}

/// <summary>
/// A previously-invoked produce failed with a non-success status (timeout, broker
/// rejected, connection dropped). The checker treats the outcome as unknown — the
/// record may or may not have reached the log.
/// </summary>
public sealed record ProduceFail : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required byte[] Value { get; init; }
    public required string Reason { get; init; }
}

/// <summary>A consumer began polling from <see cref="FromOffset"/>.</summary>
public sealed record ConsumeInvoke : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long FromOffset { get; init; }
}

/// <summary>A consume call returned a single record at a specific offset.</summary>
public sealed record ConsumeOk : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required byte[] Value { get; init; }
}

/// <summary>A consume call failed (timeout, error, broker down).</summary>
public sealed record ConsumeFail : ChaosOperation
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long FromOffset { get; init; }
    public required string Reason { get; init; }
}
