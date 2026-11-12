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
    // Emitted AFTER all IBrokerPlugin activation completes — so a plugin crashing while constructing a client never
    // reaches it, which is exactly what makes it the crash guard — and once the broker has caught up with its
    // metadata log and bound the client listener (#250): a client that connects on this line is answered from the
    // metadata the broker had before it restarted.
    public const string ReadyLine = "Surgewave broker ready to accept connections";

    // The cluster controller's word that the metadata log is applied and acted on; precedes ReadyLine.
    public const string CaughtUpLine = "has caught up with the metadata log";

    // The host's word, at the start of the wait, that it has not caught up yet and lets no client in until it has
    // (#250): what a broker says that waits for a quorum, and all it says about it until the quorum is there.
    public const string CatchingUpLine = "Catching up with the metadata log; clients on";

    // Fires only once app.RunAsync() actually binds Kestrel — also catches a TLS/dev-cert bind failure.
    public const string AppStartedLine = "Application started. Press Ctrl+C to shut down.";

    private readonly Process _process;
    private readonly StringBuilder _log = new();
    private readonly List<(string Text, TaskCompletionSource Seen)> _awaitedLines = []; // under the lock of _log
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _appStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<int> _exited = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private BrokerProcess(Process process, BrokerPorts ports)
    {
        _process = process;
        KafkaPort = ports.Kafka;
        RestPort = ports.Grpc;
        SecondEndpointPort = ports.Http3;
    }

    /// <summary>The port the broker's Kafka listener binds.</summary>
    public int KafkaPort { get; }

    /// <summary>
    /// The port of the broker's <c>Grpc</c> endpoint: the transcoded REST API, <c>/health</c> and <c>/metrics</c>,
    /// over HTTP/1.1 while the broker runs without TLS, as it does unless a test switches TLS on.
    /// </summary>
    public int RestPort { get; }

    /// <summary>
    /// The port of the broker's second endpoint: gRPC over HTTP/2 with prior knowledge without TLS, everything over
    /// HTTPS with it.
    /// </summary>
    public int SecondEndpointPort { get; }

    /// <summary>Bootstrap servers for a Kafka client.</summary>
    public string BootstrapServers => $"127.0.0.1:{KafkaPort}";

    /// <summary>Everything the broker wrote so far, for assertions and failure messages.</summary>
    public string Log
    {
        get { lock (_log) return _log.ToString(); }
    }

    /// <summary>
    /// Starts the broker on fresh ports with its data in <paramref name="dataDir"/> and waits until it is ready and
    /// Kestrel is bound; see <see cref="WaitUntilReadyAsync"/>.
    /// </summary>
    public static Task<BrokerProcess> StartAsync(string dataDir, CancellationToken cancellationToken, params string[] extraArgs)
        => StartAsync(dataDir, BrokerPorts.Reserve(), cancellationToken, extraArgs);

    /// <summary>
    /// Starts the broker on <paramref name="ports"/> chosen beforehand: a cluster's brokers name each other's ports in
    /// their configuration before any of them starts.
    /// </summary>
    public static async Task<BrokerProcess> StartAsync(string dataDir, BrokerPorts ports, CancellationToken cancellationToken, params string[] extraArgs)
    {
        var broker = Launch(dataDir, ports, extraArgs);
        try
        {
            await broker.WaitUntilReadyAsync(cancellationToken);
            return broker;
        }
        catch
        {
            await broker.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Starts the broker process on <paramref name="ports"/> and returns without waiting for it: a broker of a cluster
    /// is ready only once a majority of the controller voters is up, so the first one is launched, the next one too,
    /// and then <see cref="WaitUntilReadyAsync"/> is awaited on both.
    /// </summary>
    public static BrokerProcess Launch(string dataDir, BrokerPorts ports, params string[] extraArgs)
    {
        var brokerDll = ResolveBrokerDll();
        var (kafkaPort, grpcPort, http3Port, replPort) = (ports.Kafka, ports.Grpc, ports.Http3, ports.Replication);

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
            // The two HTTP endpoints on this broker's own ports, on loopback. Their scheme and protocols are the
            // shipped ones: Surgewave:GrpcUseTls of appsettings.json is false outside the Development environment,
            // which alone yields cleartext (#276), and a test that wants TLS adds the setting.
            $"--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:{grpcPort}",
            $"--Kestrel:Endpoints:GrpcHttp3:Url=http://127.0.0.1:{http3Port}",
            $"--Surgewave:DataDirectory={dataDir}",
            $"--Surgewave:LogDirectory={Path.Combine(dataDir, "logs")}",
            "--Surgewave:Kafka:Enabled=true",
            "--Surgewave:DefaultReplicationFactor=1",
            .. extraArgs,
        ];
        foreach (var a in args) psi.ArgumentList.Add(a);

        var broker = new BrokerProcess(new Process { StartInfo = psi, EnableRaisingEvents = true }, ports);
        broker._process.OutputDataReceived += (_, e) => broker.OnLine(e.Data);
        broker._process.ErrorDataReceived += (_, e) => broker.OnLine(e.Data);
        broker._process.Exited += (_, _) => broker._exited.TrySetResult(broker._process.ExitCode);

        try
        {
            Assert.True(broker._process.Start(), "Failed to start the broker process.");
            broker._process.BeginOutputReadLine();
            broker._process.BeginErrorReadLine();
            return broker;
        }
        catch
        {
            broker._process.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Waits until the broker is ready and Kestrel is bound. Fails the test if it exits first or does not get there in
    /// time.
    /// </summary>
    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        // Race readiness against an early crash and a hard timeout. An early exit is the
        // failure the boot tests exist to catch (a plugin blew up before the broker was ready).
        var timeout = Task.Delay(TimeSpan.FromSeconds(90), cancellationToken);
        var first = await Task.WhenAny(_ready.Task, _exited.Task, timeout);
        if (first == _exited.Task)
            Assert.Fail($"Broker exited with code {await _exited.Task} before reaching readiness.\n{Dump()}");
        if (first == timeout)
            Assert.Fail($"Broker did not reach '{ReadyLine}' within 90s.\n{Dump()}");

        // Readiness reached; require the host to actually bind Kestrel shortly after.
        await Task.WhenAny(_appStarted.Task, _exited.Task, Task.Delay(TimeSpan.FromSeconds(20), cancellationToken));
        Assert.True(_appStarted.Task.IsCompletedSuccessfully,
            $"Broker reached readiness but Kestrel did not bind ('{AppStartedLine}' never appeared).\n{Dump()}");
    }

    /// <summary>
    /// Waits until the broker has written a line that contains <paramref name="text"/>: at once if it already has.
    /// Fails the test if the broker exits first or does not say it in time.
    /// </summary>
    public async Task WaitForLineAsync(string text, CancellationToken cancellationToken)
    {
        var seen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_log)
        {
            if (_log.ToString().Contains(text, StringComparison.Ordinal))
                return;
            _awaitedLines.Add((text, seen));
        }

        var timeout = Task.Delay(TimeSpan.FromSeconds(90), cancellationToken);
        var first = await Task.WhenAny(seen.Task, _exited.Task, timeout);
        if (first == _exited.Task)
            Assert.Fail($"Broker exited with code {await _exited.Task} before it said '{text}'.\n{Dump()}");
        if (first == timeout)
            Assert.Fail($"Broker did not say '{text}' within 90s.\n{Dump()}");
    }

    /// <summary>Whether the broker has said it is ready.</summary>
    public bool HasReportedReady => _ready.Task.IsCompletedSuccessfully;

    /// <summary>The broker output with a header, for a failure message.</summary>
    public string Dump() => "--- broker output ---\n" + Log;

    private void OnLine(string? line)
    {
        if (line is null) return;
        lock (_log)
        {
            _log.AppendLine(line);
            for (var i = _awaitedLines.Count - 1; i >= 0; i--)
            {
                if (!line.Contains(_awaitedLines[i].Text, StringComparison.Ordinal))
                    continue;
                _awaitedLines[i].Seen.TrySetResult();
                _awaitedLines.RemoveAt(i);
            }
        }
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

    internal static int FreeTcpPort()
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
            .ThenBy(p => p.Length) // the build without a runtime, not one for a runtime in a directory below it
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

/// <summary>
/// The four ports a broker binds — Kafka, gRPC and REST, the second HTTP endpoint and replication — distinct, as
/// BrokerConfig requires, and ephemeral, as fixed defaults would collide across runs.
/// </summary>
internal sealed record BrokerPorts(int Kafka, int Grpc, int Http3, int Replication)
{
    public static BrokerPorts Reserve() =>
        new(BrokerProcess.FreeTcpPort(), BrokerProcess.FreeTcpPort(), BrokerProcess.FreeTcpPort(), BrokerProcess.FreeTcpPort());
}
