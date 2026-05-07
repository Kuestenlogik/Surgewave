using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// High-performance extension methods for TimeProvider using Environment.TickCount64.
/// TickCount64 is a simple memory read (~1-2ns) with no syscall overhead.
/// </summary>
public static class TimeProviderExtensions
{
    // Base offset: UnixMs at startup minus TickCount64 at startup
    // To get current Unix time: _baseOffset + Environment.TickCount64
    private static readonly long _baseOffset;

    static TimeProviderExtensions()
    {
        _baseOffset = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - Environment.TickCount64;
    }

    /// <summary>
    /// Get Unix timestamp in milliseconds using Environment.TickCount64.
    /// This is a simple memory read (~1-2ns) with no syscall overhead.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetUnixTimeMilliseconds(this TimeProvider _)
    {
        return _baseOffset + Environment.TickCount64;
    }
}

/// <summary>
/// Static cached timestamp for hot paths that don't use DI.
/// Uses Environment.TickCount64 for ~1-2ns access vs ~1µs for DateTimeOffset.UtcNow.
/// </summary>
public static class CachedTimestamp
{
    // Base offset: UnixMs at startup minus TickCount64 at startup
    private static readonly long _baseOffset;

    static CachedTimestamp()
    {
        _baseOffset = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - Environment.TickCount64;
    }

    /// <summary>
    /// Get current Unix timestamp in milliseconds.
    /// Uses Environment.TickCount64 for ~1-2ns access without syscalls.
    /// </summary>
    public static long UnixMilliseconds
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _baseOffset + Environment.TickCount64;
    }
}
