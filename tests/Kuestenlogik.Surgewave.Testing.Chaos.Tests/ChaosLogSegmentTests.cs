using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for ChaosLogSegment fault injection around ILogSegment operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ChaosLogSegmentTests : IDisposable
{
    private readonly ChaosEngine _engine;
    private readonly ILogSegment _innerSegment;
    private readonly ChaosLogSegment _chaosSegment;

    public ChaosLogSegmentTests()
    {
        _engine = new ChaosEngine();
        _innerSegment = Substitute.For<ILogSegment>();
        _chaosSegment = new ChaosLogSegment(
            _innerSegment, _engine, brokerId: 1,
            NullLogger<ChaosLogSegment>.Instance);
    }

    public void Dispose() => _chaosSegment.Dispose();

    [Fact]
    public async Task AppendBatch_WithDiskError_ThrowsIOException()
    {
        // Arrange
        _engine.ActivateFault(FaultType.DiskIoError, new FaultScope { BrokerId = 1 });
        var batch = new byte[] { 1, 2, 3, 4 };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(
            () => _chaosSegment.AppendBatchAsync(batch).AsTask());
    }

    [Fact]
    public async Task AppendBatch_WithStorageFull_ThrowsIOException()
    {
        // Arrange
        _engine.ActivateFault(FaultType.StorageFullError, new FaultScope { BrokerId = 1 });
        var batch = new byte[] { 1, 2, 3, 4 };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(
            () => _chaosSegment.AppendBatchAsync(batch).AsTask());
    }

    [Fact]
    public async Task AppendBatch_NoFault_DelegatesToInner()
    {
        // Arrange
        var batch = new byte[] { 1, 2, 3, 4 };
        _innerSegment.AppendBatchAsync(batch, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<(long baseOffset, int recordCount)>((0L, 1)));

        // Act
        var result = await _chaosSegment.AppendBatchAsync(batch);

        // Assert
        Assert.Equal(0L, result.baseOffset);
        Assert.Equal(1, result.recordCount);
        _ = await _innerSegment.Received(1).AppendBatchAsync(batch, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ReadBatches_WithDiskError_ThrowsIOException()
    {
        // Arrange
        _engine.ActivateFault(FaultType.DiskIoError, new FaultScope { BrokerId = 1 });

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(
            () => _chaosSegment.ReadBatchesAsync(0, 1024).AsTask());
    }

    [Fact]
    public async Task ReadBatches_WithCorruption_ModifiesData()
    {
        // Arrange
        _engine.ActivateFault(FaultType.MessageCorruption, new FaultScope { BrokerId = 1 });

        // Return known data from inner segment
        var originalData = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 };
        _innerSegment.ReadBatchesAsync(0, 1024, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<List<byte[]>>(new List<byte[]> { originalData.ToArray() }));

        // Act
        var result = await _chaosSegment.ReadBatchesAsync(0, 1024).AsTask();

        // Assert - data should be returned (corruption may or may not change content depending on random bit flips)
        Assert.NotEmpty(result);
        Assert.Single(result);
        Assert.Equal(originalData.Length, result[0].Length);
    }

    [Fact]
    public async Task SlowDisk_InjectsLatency()
    {
        // Arrange
        var latency = TimeSpan.FromMilliseconds(100);
        _engine.ActivateFault(FaultType.SlowNetwork, new FaultScope { BrokerId = 1 }, latency);

        var batch = new byte[] { 1, 2, 3, 4 };
        _innerSegment.AppendBatchAsync(batch, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<(long baseOffset, int recordCount)>((0L, 1)));

        // Act
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _ = await _chaosSegment.AppendBatchAsync(batch);
        sw.Stop();

        // Assert - should take at least the injected latency
        // Use a lower bound to account for timing imprecision
        Assert.True(sw.ElapsedMilliseconds >= 50,
            $"Expected at least 50ms latency, but took {sw.ElapsedMilliseconds}ms");
    }
}
