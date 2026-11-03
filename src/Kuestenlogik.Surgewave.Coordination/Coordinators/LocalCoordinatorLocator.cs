namespace Kuestenlogik.Surgewave.Coordination.Coordinators;

/// <summary>
/// A broker on its own — no cluster — coordinates every group and transactional id itself, at the address it
/// advertises.
/// </summary>
public sealed class LocalCoordinatorLocator(int brokerId, Func<string> host, Func<int> port) : ICoordinatorLocator
{
    /// <inheritdoc />
    public CoordinatorLocation Locate(CoordinatorKind kind, string key) =>
        new(CoordinatorLookupStatus.Ok, brokerId, host(), port());

    /// <inheritdoc />
    public bool IsLocal(CoordinatorKind kind, string key) => true;
}
