using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for measuring startup time and initialization overhead.
/// These benchmarks help evaluate the impact of AOT compilation.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess, invocationCount: 1, iterationCount: 5)]
[MemoryDiagnoser]
[BenchmarkCategory("Unit", "Startup")]
public class StartupBenchmarks
{
    private string _tempDirectory = null!;

    [IterationSetup]
    public void IterationSetup()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), "surgewave-startup-bench", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirectory);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        try
        {
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    /// <summary>
    /// Measure LogManager initialization time
    /// </summary>
    [Benchmark]
    public LogManager LogManagerStartup()
    {
        var retentionPolicy = new RetentionPolicy
        {
            RetentionHours = 168,
            RetentionBytes = -1
        };
        var logManager = new LogManager(_tempDirectory, retentionPolicy: retentionPolicy);
        logManager.Dispose();
        return logManager;
    }

    /// <summary>
    /// Measure FileStorageEngine creation time
    /// </summary>
    [Benchmark]
    public ILogSegment FileStorageEngineCreation()
    {
        using var engine = new FileStorageEngine(_tempDirectory, baseOffset: 0, createNew: true);
        var segment = new StorageEngineSegmentAdapter(engine);
        segment.Dispose();
        return segment;
    }

    /// <summary>
    /// Measure LoggerFactory creation time (common startup overhead)
    /// </summary>
    [Benchmark]
    public ILoggerFactory LoggerFactoryCreation()
    {
        var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Information);
        });
        factory.Dispose();
        return factory;
    }

    /// <summary>
    /// Measure full component initialization sequence
    /// </summary>
    [Benchmark]
    public void FullInitSequence()
    {
        // Simulate broker startup sequence
        var retentionPolicy = new RetentionPolicy
        {
            RetentionHours = 168,
            RetentionBytes = -1
        };

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
        });

        using var logManager = new LogManager(_tempDirectory, retentionPolicy: retentionPolicy);

        // Create a few partitions
        for (int i = 0; i < 3; i++)
        {
            var partitionDir = Path.Combine(_tempDirectory, $"test-topic-{i}");
            Directory.CreateDirectory(partitionDir);
            using var engine = new FileStorageEngine(partitionDir, baseOffset: 0, createNew: true);
            using var segment = new StorageEngineSegmentAdapter(engine);
        }
    }
}

/// <summary>
/// Process startup benchmark - measures actual process startup time.
/// This requires publishing the broker first.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess, invocationCount: 1, iterationCount: 3)]
public class ProcessStartupBenchmarks
{
    private string? _brokerExePath;

    [GlobalSetup]
    public void Setup()
    {
        // Check if broker exe exists (for AOT testing)
        var possiblePaths = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "Kuestenlogik.Surgewave.Broker", "bin", "Release", "net10.0", "surgewave-broker.exe"),
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "Kuestenlogik.Surgewave.Broker", "bin", "Release", "net10.0", "publish", "surgewave-broker.exe"),
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "artifacts", "publish", "Kuestenlogik.Surgewave.Broker", "release", "surgewave-broker.exe"),
        };

        foreach (var path in possiblePaths)
        {
            var normalized = Path.GetFullPath(path);
            if (File.Exists(normalized))
            {
                _brokerExePath = normalized;
                break;
            }
        }
    }

    /// <summary>
    /// Measure time to start broker process and receive first response.
    /// Only runs if broker executable is found.
    /// </summary>
    [Benchmark]
    public TimeSpan? MeasureBrokerStartupTime()
    {
        if (_brokerExePath == null)
        {
            return null; // Skip if broker not published
        }

        var sw = Stopwatch.StartNew();
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-startup-test", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = _brokerExePath,
                Arguments = $"--data-directory \"{tempDir}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(psi);
            if (process == null) return null;

            // Wait for startup or timeout after 10 seconds
            var startTime = sw.Elapsed;
            var ready = false;

            while (sw.Elapsed < TimeSpan.FromSeconds(10) && !ready)
            {
                try
                {
                    using var client = new System.Net.Sockets.TcpClient();
                    client.Connect("localhost", 9092);
                    ready = true;
                }
                catch
                {
                    Thread.Sleep(10);
                }
            }

            var startupTime = sw.Elapsed;

            // Kill the process
            try { process.Kill(); } catch { }

            return ready ? startupTime : null;
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }
}
