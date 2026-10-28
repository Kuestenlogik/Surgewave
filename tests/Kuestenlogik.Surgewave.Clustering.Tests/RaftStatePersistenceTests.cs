using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Term and vote are saved from several Raft paths at once (#184). Unserialised, two saves collided on the same
/// temp file ("being used by another process"), and an older save landing last took the term back — a node
/// that restarts on a lower term can vote twice in a term it already voted in.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftStatePersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "surgewave-raft-state-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RaftPersistence NewPersistence() =>
        new(NullLogger<RaftPersistence>.Instance, new ClusteringConfig { RaftDataDirectory = _dir });

    [Fact]
    public async Task ConcurrentSavesNeitherCollideNorGoBackwards()
    {
        var persistence = NewPersistence();
        var ct = TestContext.Current.CancellationToken;

        await Task.WhenAll(Enumerable.Range(1, 200).Select(term =>
            Task.Run(() => persistence.SaveStateAsync(term, votedFor: term % 3, ct), ct)));

        var reloaded = await NewPersistence().LoadStateAsync(ct);
        Assert.Equal(200, reloaded.CurrentTerm);
        Assert.Equal(200 % 3, reloaded.VotedFor);
    }

    [Fact]
    public async Task AnOlderStateDoesNotOverwriteANewerOne()
    {
        var persistence = NewPersistence();
        var ct = TestContext.Current.CancellationToken;

        await persistence.SaveStateAsync(7, votedFor: 2, ct);
        await persistence.SaveStateAsync(6, votedFor: 1, ct);      // a lower term
        await persistence.SaveStateAsync(7, votedFor: null, ct);   // the same term without its vote

        var reloaded = await NewPersistence().LoadStateAsync(ct);
        Assert.Equal(new RaftPersistentState(7, 2), reloaded);
    }

    [Fact]
    public async Task AfterARestartTheLoadedStateIsTheFloor()
    {
        var ct = TestContext.Current.CancellationToken;
        await NewPersistence().SaveStateAsync(9, votedFor: 3, ct);

        var restarted = NewPersistence();
        await restarted.LoadStateAsync(ct);
        await restarted.SaveStateAsync(4, votedFor: 1, ct);

        Assert.Equal(new RaftPersistentState(9, 3), await NewPersistence().LoadStateAsync(ct));
    }
}
