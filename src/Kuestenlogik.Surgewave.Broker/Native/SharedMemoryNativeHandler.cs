// Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
// This file requires the Kuestenlogik.Surgewave.Transport.SharedMemory package.
// Install it to enable multi-client shared memory transport handling.

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Information about a connected shared memory client.
/// </summary>
public sealed class SharedMemoryClientInfo
{
    public Guid ClientId { get; init; }
    public DateTime ConnectedAt { get; init; }
    public long MessagesProcessed { get; init; }
    public bool IsRunning { get; init; }
}
