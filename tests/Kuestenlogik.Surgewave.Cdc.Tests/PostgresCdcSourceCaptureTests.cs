using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for <see cref="PostgresCdcSource.CaptureChangesAsync"/> failure behavior that
/// requires no live PostgreSQL. Pins that connection-string problems surface lazily on
/// the first enumeration step (never at construction or enumerator creation) and that
/// a failed capture attempt leaves the progress counters untouched.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PostgresCdcSourceCaptureTests
{
    /// <summary>
    /// A connection string with an unknown keyword: Npgsql rejects it with an
    /// <see cref="ArgumentException"/> during connection setup, before any
    /// network activity happens.
    /// </summary>
    private const string MalformedConnectionString = "ThisKeywordDoesNotExist=1";

    private static PostgresCdcSource CreateSource(string connectionString)
    {
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        var config = new CdcConfig { ConnectionString = connectionString };
        return new PostgresCdcSource(config, loggerFactory.CreateLogger<PostgresCdcSource>());
    }

    [Fact]
    public void Constructor_MalformedConnectionString_DoesNotThrow()
    {
        // The connection string is not parsed at construction time; only the
        // slot/publication identifiers are pre-processed.
        var source = CreateSource(MalformedConnectionString);

        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public async Task CaptureChangesAsync_EnumeratorCreation_IsLazy()
    {
        var source = CreateSource(MalformedConnectionString);

        // Creating the enumerable and its enumerator must not perform any
        // connection work; the failure may only surface on the first MoveNextAsync.
        var enumerable = source.CaptureChangesAsync();
        var enumerator = enumerable.GetAsyncEnumerator();

        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await enumerator.MoveNextAsync());

        await enumerator.DisposeAsync();
        await source.DisposeAsync();
    }

    [Fact]
    public async Task CaptureChangesAsync_MalformedConnectionString_ThrowsAndCapturesNothing()
    {
        var source = CreateSource(MalformedConnectionString);

        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
        {
            await foreach (var evt in source.CaptureChangesAsync())
            {
                Assert.Fail($"No event should ever be produced, but got {evt.Operation} on {evt.Schema}.{evt.Table}");
            }
        });

        Assert.Equal(0, source.EventsCaptured);
        Assert.Equal(0, source.LastConfirmedLsn);
        await source.DisposeAsync();
    }
}
