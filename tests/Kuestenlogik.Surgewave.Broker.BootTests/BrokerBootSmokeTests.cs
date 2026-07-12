using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// Full-host boot smoke test: launches the REAL built broker executable
/// (<c>surgewave-broker.dll</c>) as a subprocess with Connect enabled and asserts it boots to
/// "ready" without crashing, then shuts it down.
///
/// <para>
/// Why a subprocess and not the embedded runtime: every other broker test boots the lightweight
/// in-process <c>SurgewaveRuntime</c>, which never runs the real <c>WebApplication</c> host, the
/// <c>BrokerPluginActivator</c>, or the WebApplication-based broker plugins (e.g.
/// <c>SurgewaveConnectBrokerPlugin</c>). That gap once let a startup crash pass CI green while the
/// actual <c>dotnet run</c> broker crashed: #59 moved the Kafka wire client into the optional
/// <c>Client.Kafka</c> assembly, and the Connect broker plugin built a Kafka-protocol client at
/// startup that then required that (absent) assembly. Only a real process boot exercises the full
/// plugin-activation closure, so only a subprocess test guards against that class of regression.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class BrokerBootSmokeTests
{
    // Emitted synchronously AFTER all IBrokerPlugin activation completes — so a plugin crashing
    // while constructing a client never reaches it, which is exactly what makes it the crash guard.
    private const string ReadyLine = "Surgewave broker ready to accept connections";

    // Fires only once app.RunAsync() actually binds Kestrel — also catches a TLS/dev-cert bind failure.
    private const string AppStartedLine = "Application started. Press Ctrl+C to shut down.";

    [Fact(Timeout = 180_000)]
    public async Task Broker_BootsToReady_WithConnectEnabled()
    {
        var brokerDll = ResolveBrokerDll();
        var brokerDir = Path.GetDirectoryName(brokerDll)!;
        EnsureKafkaPluginStaged(brokerDir);

        // Four distinct ephemeral ports: BrokerConfig requires Port, GrpcPort and ReplicationPort
        // to all differ, and fixed defaults (9092/9093/10092/9094) would collide across runs.
        int kafkaPort = FreeTcpPort(), grpcPort = FreeTcpPort(), http3Port = FreeTcpPort(), replPort = FreeTcpPort();
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);

        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = brokerDir,
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
            "--Surgewave:Connect:Enabled=true", // the regression's trigger — force it on regardless of appsettings
            "--Surgewave:Kafka:Enabled=true",
            "--Surgewave:AutoCreateTopics=true",
            "--Surgewave:DefaultReplicationFactor=1",
        ];
        foreach (var a in args) psi.ArgumentList.Add(a);

        var log = new StringBuilder();
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var appStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var exited = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        void OnLine(string? line)
        {
            if (line is null) return;
            lock (log) log.AppendLine(line);
            if (line.Contains(ReadyLine, StringComparison.Ordinal)) ready.TrySetResult();
            if (line.Contains(AppStartedLine, StringComparison.Ordinal)) appStarted.TrySetResult();
        }

        using var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };
        proc.OutputDataReceived += (_, e) => OnLine(e.Data);
        proc.ErrorDataReceived += (_, e) => OnLine(e.Data);
        proc.Exited += (_, _) => exited.TrySetResult(proc.ExitCode);

        try
        {
            Assert.True(proc.Start(), "Failed to start the broker process.");
            proc.BeginOutputReadLine();
            proc.BeginErrorReadLine();

            // Race readiness against an early crash and a hard timeout. An early exit is the
            // failure this test exists to catch (a plugin blew up before the broker was ready).
            var timeout = Task.Delay(TimeSpan.FromSeconds(90));
            var first = await Task.WhenAny(ready.Task, exited.Task, timeout);
            if (first == exited.Task)
                Assert.Fail($"Broker exited with code {await exited.Task} before reaching readiness.\n{Dump(log)}");
            if (first == timeout)
                Assert.Fail($"Broker did not reach '{ReadyLine}' within 90s.\n{Dump(log)}");

            // Readiness reached; require the host to actually bind Kestrel shortly after.
            await Task.WhenAny(appStarted.Task, exited.Task, Task.Delay(TimeSpan.FromSeconds(20)));
            Assert.True(appStarted.Task.IsCompletedSuccessfully,
                $"Broker reached readiness but Kestrel did not bind ('{AppStartedLine}' never appeared).\n{Dump(log)}");
        }
        finally
        {
            await StopAsync(proc);
            TryDelete(dataDir);
        }
    }

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private static string ResolveBrokerDll()
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

    // Mirror the b6 CopyKafkaPluginForDevRun target: if a parallel/incremental build left the
    // broker's plugins/ without the Kafka plugin, stage it here so the boot also exercises real
    // plugin discovery. Best-effort — the Connect-plugin crash this test guards reproduces even
    // native-only, but staging makes the coverage complete.
    private static void EnsureKafkaPluginStaged(string brokerDir)
    {
        var pluginsDir = Path.Combine(brokerDir, "plugins");
        if (File.Exists(Path.Combine(pluginsDir, "Kuestenlogik.Surgewave.Protocol.Kafka.dll")))
            return;
        var binDir = Directory.GetParent(brokerDir)?.Parent?.FullName; // .../artifacts/bin
        if (binDir is null) return;
        var config = new DirectoryInfo(brokerDir).Name;
        var pkOut = Path.Combine(binDir, "Kuestenlogik.Surgewave.Protocol.Kafka", config);
        if (!Directory.Exists(pkOut)) return;
        Directory.CreateDirectory(pluginsDir);
        foreach (var f in Directory.GetFiles(pkOut))
        {
            var name = Path.GetFileName(f);
            var keep = name.Equals("Kuestenlogik.Surgewave.Protocol.Kafka.dll", StringComparison.OrdinalIgnoreCase)
                || name.Equals("plugin.json", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith(".deps.json", StringComparison.OrdinalIgnoreCase)
                // the plugin's private, non-shared external deps (shared Kuestenlogik.* already ship in the broker bin)
                || (name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
                    && !name.StartsWith("Kuestenlogik.Surgewave.", StringComparison.OrdinalIgnoreCase));
            if (keep)
                try { File.Copy(f, Path.Combine(pluginsDir, name), overwrite: true); } catch { /* best-effort */ }
        }
    }

    private static async Task StopAsync(Process proc)
    {
        try
        {
            if (proc.HasExited) return;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // CTRL_C to a child console process is unreliable on Windows; kill the tree.
                proc.Kill(entireProcessTree: true);
            }
            else
            {
                // Graceful SIGTERM: the .NET console lifetime runs ApplicationStopping + disposes cleanly.
                using var kill = Process.Start(new ProcessStartInfo("/bin/kill", $"-s TERM {proc.Id}") { UseShellExecute = false });
                kill?.WaitForExit(5_000);
            }
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await proc.WaitForExitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) { if (!proc.HasExited) proc.Kill(entireProcessTree: true); }
        }
        catch { /* best-effort teardown; never mask the test's own assertion */ }
    }

    private static string Dump(StringBuilder log)
    {
        lock (log) { return "--- broker output ---\n" + log; }
    }

    private static void TryDelete(string dir)
    {
        try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
        catch { /* a leftover temp dir is harmless; Windows file locks can linger briefly */ }
    }
}

[CollectionDefinition("BrokerBoot", DisableParallelization = true)]
public sealed class BrokerBootCollection;
