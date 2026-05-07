using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Utility for setting thread CPU affinity for low-latency I/O threads.
/// CPU affinity pins a thread to specific cores, reducing context switches
/// and improving cache locality.
/// </summary>
public static class ThreadAffinity
{
    private static int s_nextCore;
    private static readonly int s_coreCount = Environment.ProcessorCount;

    /// <summary>
    /// Sets the current thread's affinity to a specific CPU core.
    /// Uses round-robin assignment if core is -1.
    /// </summary>
    /// <param name="core">CPU core index (0-based), or -1 for auto-assign</param>
    /// <returns>True if affinity was set successfully</returns>
    public static bool SetCurrentThreadAffinity(int core = -1)
    {
        if (core < 0)
        {
            // Round-robin assignment across cores
            core = Interlocked.Increment(ref s_nextCore) % s_coreCount;
        }

        if (core >= s_coreCount)
        {
            return false;
        }

        if (OperatingSystem.IsWindows())
        {
            return SetWindowsThreadAffinity(core);
        }
        else if (OperatingSystem.IsLinux())
        {
            return SetLinuxThreadAffinity(core);
        }

        // macOS doesn't support thread affinity (uses thread_policy_set with different semantics)
        return false;
    }

    [SupportedOSPlatform("windows")]
    private static bool SetWindowsThreadAffinity(int core)
    {
        try
        {
            var affinityMask = (nint)(1UL << core);
            var result = SetThreadAffinityMask(GetCurrentThread(), affinityMask);
            return result != 0;
        }
        catch
        {
            return false;
        }
    }

    [SupportedOSPlatform("linux")]
    private static bool SetLinuxThreadAffinity(int core)
    {
        try
        {
            // Use cpu_set_t (128 bytes on most Linux systems)
            Span<byte> cpuSet = stackalloc byte[128];
            cpuSet.Clear();

            // Set the bit for our core
            var byteIndex = core / 8;
            var bitIndex = core % 8;
            if (byteIndex < cpuSet.Length)
            {
                cpuSet[byteIndex] = (byte)(1 << bitIndex);
            }

            unsafe
            {
                fixed (byte* ptr = cpuSet)
                {
                    // sched_setaffinity(0 = current thread, size, cpu_set)
                    var result = sched_setaffinity(0, (nuint)cpuSet.Length, ptr);
                    return result == 0;
                }
            }
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Gets the number of available CPU cores
    /// </summary>
    public static int CoreCount => s_coreCount;

    // Windows P/Invoke
    [DllImport("kernel32.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [SupportedOSPlatform("windows")]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [SupportedOSPlatform("windows")]
    private static extern nint SetThreadAffinityMask(nint hThread, nint dwThreadAffinityMask);

    // Linux P/Invoke
    [DllImport("libc", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [SupportedOSPlatform("linux")]
    private static extern unsafe int sched_setaffinity(int pid, nuint cpusetsize, byte* cpuset);
}

/// <summary>
/// Creates a dedicated thread with optional CPU affinity for low-latency operations.
/// Unlike Task.Run which uses thread pool threads, this creates a long-running
/// thread that won't be preempted by other work.
/// </summary>
public sealed class DedicatedThread : IDisposable
{
    private readonly Thread _thread;
    private readonly CancellationTokenSource _cts = new();
    private readonly TaskCompletionSource _started = new();
    private readonly TaskCompletionSource _completed = new();
    private bool _disposed;

    public CancellationToken CancellationToken => _cts.Token;

    /// <summary>
    /// Creates and starts a dedicated thread.
    /// </summary>
    /// <param name="action">Action to run on the thread</param>
    /// <param name="name">Thread name for debugging</param>
    /// <param name="cpuAffinity">CPU core to pin to (-1 for auto-assign, null for no affinity)</param>
    /// <param name="priority">Thread priority</param>
    public DedicatedThread(
        Action<CancellationToken> action,
        string? name = null,
        int? cpuAffinity = -1,
        ThreadPriority priority = ThreadPriority.AboveNormal)
    {
        _thread = new Thread(() =>
        {
            // Set affinity if requested
            if (cpuAffinity.HasValue)
            {
                ThreadAffinity.SetCurrentThreadAffinity(cpuAffinity.Value);
            }

            _started.SetResult();

            try
            {
                action(_cts.Token);
                _completed.SetResult();
            }
            catch (OperationCanceledException)
            {
                _completed.SetResult();
            }
            catch (Exception ex)
            {
                _completed.SetException(ex);
            }
        })
        {
            IsBackground = true,
            Priority = priority,
            Name = name ?? "DedicatedThread"
        };

        _thread.Start();
    }

    /// <summary>
    /// Wait for the thread to start executing
    /// </summary>
    public Task WaitForStartAsync() => _started.Task;

    /// <summary>
    /// Wait for the thread to complete
    /// </summary>
    public Task WaitForCompletionAsync() => _completed.Task;

    /// <summary>
    /// Signal the thread to stop
    /// </summary>
    public void Stop() => _cts.Cancel();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts.Cancel();

        // Wait for thread to complete with timeout
        if (!_thread.Join(TimeSpan.FromSeconds(5)))
        {
            // Thread didn't stop in time - nothing we can safely do
        }

        _cts.Dispose();
    }
}
