using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The real built broker (<c>surgewave-broker.dll</c>) as a subprocess on free ports and a given data directory.
/// Readiness is the broker's own log line, never a probe.
/// </summary>
internal sealed class BrokerProcess : IAsyncDisposable
{
    // Emitted synchronously AFTER all IBrokerPlugin activation completes — so a plugin crashing
    // while constructing a client never reaches it, which is exactly what makes it the crash guard.
    public const string ReadyLine = "Surgewave broker ready to accept connections";

    // Fires only once app.RunAsync() actually binds Kestrel — also catches a TLS/dev-cert bind failure.
    public const string AppStartedLine = "Application started. Press Ctrl+C to shut down.";

    private readonly Process _process;
    private readonly StringBuilder _log = new();
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _appStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<int> _exited = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private BrokerProcess(Process process, int kafkaPort)
    {
        _process = process;
        KafkaPort = kafkaPort;
    }

    /// <summary>The port the broker's Kafka listener binds.</summary>
    public int KafkaPort { get; }

    /// <summary>Bootstrap servers for a Kafka client.</summary>
    public string BootstrapServers => $"127.0.0.1:{KafkaPort}";

    /// <summary>Everything the broker wrote so far, for assertions and failure messages.</summary>
    public string Log
    {
        get { lock (_log) return _log.ToString(); }
    }

    /// <summary>
    /// Starts the broker on fresh ports with its data in <paramref name="dataDir"/> and waits until it is ready and
    /// Kestrel is bound. Fails the test if it exits first or does not get there in time.
    /// </summary>
    public static async Task<BrokerProcess> StartAsync(string dataDir, CancellationToken cancellationToken, params string[] extraArgs)
    {
        var brokerDll = ResolveBrokerDll();

        // Four distinct ephemeral ports: BrokerConfig requires Port, GrpcPort and ReplicationPort
        // to all differ, and fixed defaults (9092/9093/10092/9094) would collide across runs.
        int kafkaPort = FreeTcpPort(), grpcPort = FreeTcpPort(), http3Port = FreeTcpPort(), replPort = FreeTcpPort();

        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(brokerDll)!,
        };
        psi.Environment["NO_COLOR"] = "1";
        psi.Environment["DOTNET_ENVIRONMENT"] = "Production";
        string[] args =
        [
            brokerDll,
            "--Surgewave:Host=127.0.0.1",
            $"--Surgewave:Port={kafkaPort}",
            $"--Surgewave:GrpcPort={grpcPort}",
            $"--Surgewave:ReplicationPort={replPort}",
            "--Surgewave:GrpcUseTls=false",
            // appsettings.json pins the two admin/gRPC Kestrel endpoints to https, which a
            // dev-cert-less CI runner cannot bind. GrpcUseTls=false alone does not undo that, so
            // override both endpoint URLs to plaintext http and drop HTTP/3 (which requires TLS).
            // Readiness is a stdout line, never an HTTP probe, so http is sufficient.
            $"--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:{grpcPort}",
            $"--Kestrel:Endpoints:GrpcHttp3:Url=http://127.0.0.1:{http3Port}",
            "--Kestrel:Endpoints:GrpcHttp3:Protocols=Http1AndHttp2",
            $"--Surgewave:DataDirectory={dataDir}",
            $"--Surgewave:LogDirectory={Path.Combine(dataDir, "logs")}",
            "--Surgewave:Kafka:Enabled=true",
            "--Surgewave:DefaultReplicationFactor=1",
            .. extraArgs,
        ];
        foreach (var a in args) psi.ArgumentList.Add(a);

        var broker = new BrokerProcess(new Process { StartInfo = psi, EnableRaisingEvents = true }, kafkaPort);
        broker._process.OutputDataReceived += (_, e) => broker.OnLine(e.Data);
        broker._process.ErrorDataReceived += (_, e) => broker.OnLine(e.Data);
        broker._process.Exited += (_, _) => broker._exited.TrySetResult(broker._process.ExitCode);

        try
        {
            Assert.True(broker._process.Start(), "Failed to start the broker process.");
            broker._process.BeginOutputReadLine();
            broker._process.BeginErrorReadLine();

            // Race readiness against an early crash and a hard timeout. An early exit is the
            // failure the boot tests exist to catch (a plugin blew up before the broker was ready).
            var timeout = Task.Delay(TimeSpan.FromSeconds(90), cancellationToken);
            var first = await Task.WhenAny(broker._ready.Task, broker._exited.Task, timeout);
            if (first == broker._exited.Task)
                Assert.Fail($"Broker exited with code {await broker._exited.Task} before reaching readiness.\n{broker.Dump()}");
            if (first == timeout)
                Assert.Fail($"Broker did not reach '{ReadyLine}' within 90s.\n{broker.Dump()}");

            // Readiness reached; require the host to actually bind Kestrel shortly after.
            await Task.WhenAny(broker._appStarted.Task, broker._exited.Task, Task.Delay(TimeSpan.FromSeconds(20), cancellationToken));
            Assert.True(broker._appStarted.Task.IsCompletedSuccessfully,
                $"Broker reached readiness but Kestrel did not bind ('{AppStartedLine}' never appeared).\n{broker.Dump()}");
            return broker;
        }
        catch
        {
            await broker.DisposeAsync();
            throw;
        }
    }

    /// <summary>The broker output with a header, for a failure message.</summary>
    public string Dump() => "--- broker output ---\n" + Log;

    private void OnLine(string? line)
    {
        if (line is null) return;
        lock (_log) _log.AppendLine(line);
        if (line.Contains(ReadyLine, StringComparison.Ordinal)) _ready.TrySetResult();
        if (line.Contains(AppStartedLine, StringComparison.Ordinal)) _appStarted.TrySetResult();
    }

    /// <summary>Stops the broker: SIGTERM where there is one, the process tree killed on Windows.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_process.HasExited) return;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // CTRL_C to a child console process is unreliable on Windows; kill the tree.
                _process.Kill(entireProcessTree: true);
            }
            else
            {
                // Graceful SIGTERM: the .NET console lifetime runs ApplicationStopping + disposes cleanly.
                using var kill = Process.Start(new ProcessStartInfo("/bin/kill", $"-s TERM {_process.Id}") { UseShellExecute = false });
                kill?.WaitForExit(5_000);
            }
            // 30s cap on a graceful exit; past it the tree is killed, so teardown still terminates the broker.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await _process.WaitForExitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) { if (!_process.HasExited) _process.Kill(entireProcessTree: true); }
        }
        catch { /* best-effort teardown; never mask the test's own assertion */ }
        finally
        {
            _process.Dispose();
        }
    }

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    internal static string ResolveBrokerDll()
    {
        var artifacts = FindArtifactsRoot();
        var config = new DirectoryInfo(AppContext.BaseDirectory).Name; // Debug / Release
        var sep = Path.DirectorySeparatorChar;
        var matches = Directory.GetFiles(artifacts, "surgewave-broker.dll", SearchOption.AllDirectories)
            .Where(p => p.Contains($"{sep}bin{sep}", StringComparison.OrdinalIgnoreCase))          // not the obj/ copy
            .Where(p => p.Contains("Kuestenlogik.Surgewave.Broker", StringComparison.OrdinalIgnoreCase))
            .Where(p => !p.Contains("Tests", StringComparison.OrdinalIgnoreCase))
            .Where(p => File.Exists(Path.Combine(Path.GetDirectoryName(p)!, "surgewave-broker.runtimeconfig.json")))
            .OrderByDescending(p => p.Contains($"{sep}{config}{sep}", StringComparison.OrdinalIgnoreCase)) // prefer our config
            .ToList();
        Assert.True(matches.Count > 0,
            $"surgewave-broker.dll not found under '{artifacts}'. Was Kuestenlogik.Surgewave.Broker built?");
        return matches[0];
    }

    private static string FindArtifactsRoot()
    {
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
        {
            if (string.Equals(d.Name, "artifacts", StringComparison.OrdinalIgnoreCase)) return d.FullName;
            var candidate = Path.Combine(d.FullName, "artifacts");
            if (Directory.Exists(candidate)) return candidate;
        }
        throw new DirectoryNotFoundException(
            $"Could not locate the 'artifacts' output root walking up from {AppContext.BaseDirectory}.");
    }

    /// <summary>Removes a test's data directory; a leftover is harmless.</summary>
    public static void TryDelete(string dir)
    {
        try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
        catch { /* a leftover temp dir is harmless; Windows file locks can linger briefly */ }
    }
}
