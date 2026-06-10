using System.Runtime.InteropServices;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// Captures the hardware + runtime profile of the machine the
/// benchmarks ran on. Gets serialised into every result file so a
/// user running on their own box can meaningfully <c>--compare</c>
/// their numbers against the reference baselines committed under
/// <c>benchmarks/baselines/</c>.
///
/// Intentionally minimal — anything that's hard to capture portably
/// (NUMA topology, exact memory speed, page cache state) stays out.
/// We capture what changes the numbers most: CPU model + count, OS
/// kernel, RAM size, and the runtime build.
/// </summary>
public sealed record HardwareFingerprint(
    string CpuModel,
    int LogicalCores,
    long TotalMemoryBytes,
    string OperatingSystem,
    string KernelVersion,
    string Architecture,
    string DotnetRuntime,
    bool IsContainerised)
{
    public static HardwareFingerprint Capture()
    {
        var cpu = TryReadCpuModel() ?? RuntimeInformation.OSArchitecture.ToString();
        var ram = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        var os = RuntimeInformation.OSDescription;
        var kernel = Environment.OSVersion.Version.ToString();
        var arch = RuntimeInformation.OSArchitecture.ToString();
        var runtime = RuntimeInformation.FrameworkDescription;
        var inContainer = File.Exists("/.dockerenv") ||
                          (Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true");

        return new HardwareFingerprint(
            CpuModel: cpu.Trim(),
            LogicalCores: Environment.ProcessorCount,
            TotalMemoryBytes: ram,
            OperatingSystem: os,
            KernelVersion: kernel,
            Architecture: arch,
            DotnetRuntime: runtime,
            IsContainerised: inContainer);
    }

    /// <summary>
    /// Best-effort CPU model lookup. /proc/cpuinfo on Linux, WMI on
    /// Windows would be heavier than this is worth — we accept that
    /// Windows shows the architecture string instead of the SKU.
    /// </summary>
    private static string? TryReadCpuModel()
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && File.Exists("/proc/cpuinfo"))
            {
                foreach (var line in File.ReadLines("/proc/cpuinfo"))
                {
                    if (line.StartsWith("model name", StringComparison.Ordinal))
                    {
                        var idx = line.IndexOf(':', StringComparison.Ordinal);
                        if (idx > 0) return line[(idx + 1)..].Trim();
                    }
                }
            }
        }
        catch
        {
            // Fall through to the architecture-string fallback.
        }
        return null;
    }

    public string ToMarkdownTable()
    {
        var ramGib = TotalMemoryBytes / 1024.0 / 1024.0 / 1024.0;
        return $$"""
                 | Field | Value |
                 |---|---|
                 | CPU | `{{CpuModel}}` |
                 | Logical cores | {{LogicalCores}} |
                 | RAM | {{ramGib:F1}} GiB |
                 | OS | {{OperatingSystem}} |
                 | Kernel | {{KernelVersion}} |
                 | Arch | {{Architecture}} |
                 | Runtime | {{DotnetRuntime}} |
                 | Containerised | {{IsContainerised}} |
                 """;
    }
}
