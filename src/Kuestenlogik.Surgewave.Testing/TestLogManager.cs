using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;

namespace Kuestenlogik.Surgewave.Testing;

/// <summary>
/// Factory for creating in-memory LogManager instances for testing.
/// Removes the need for test projects to reference Kuestenlogik.Surgewave.Storage.Engine.Memory directly.
/// </summary>
public static class TestLogManager
{
    /// <summary>
    /// Creates a LogManager backed by in-memory storage. No disk I/O, no persistence.
    /// </summary>
    /// <param name="dataDirectory">Optional data directory path (used as identifier, no files written).</param>
    public static LogManager CreateInMemory(string? dataDirectory = null)
    {
        var dir = dataDirectory ?? Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}");
        return new LogManager(dir, new MemoryLogSegmentFactory());
    }
}
