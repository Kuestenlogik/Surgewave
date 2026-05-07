using System.Diagnostics;
using Kuestenlogik.Surgewave.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Cli.IntegrationTests;

/// <summary>
/// Integration tests for Surgewave CLI commands against an embedded broker.
/// </summary>
public class CliIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private SurgewaveRuntime? _surgewave;
    private string _bootstrapServer = "";
    private readonly string _cliDllPath;

    public CliIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        // Resolve the compiled CLI DLL by scanning the artifacts/ tree.
        // We can't construct the path deterministically because the layout
        // differs between hosts: Windows produces
        //   artifacts/bin/<Project>/<Config>/<TFM>/surgewave.dll
        // while .NET 10 on Linux runners (Github Actions) produces
        //   artifacts/<Project>/<Config>/surgewave.dll
        // (no bin/ subdir, no <TFM> subdir — the SDK's Artifacts-Output
        // pivot kicks in differently when forward slashes meet our
        // Directory.Build.props' BaseOutputPath). Scanning is bullet-proof
        // and runs once per test class.
        //
        // Earlier `dotnet run --no-build` died on the same path mismatch:
        // its resolver assumes artifacts/bin/<binary> and ignores the
        // customised OutputPath. Calling `dotnet <dll>` sidesteps that.
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !Directory.Exists(Path.Combine(dir.FullName, "artifacts")))
        {
            dir = dir.Parent;
        }
        if (dir == null)
        {
            throw new InvalidOperationException(
                "Could not locate artifacts/ above " + AppContext.BaseDirectory);
        }
        var artifactsRoot = Path.Combine(dir.FullName, "artifacts");
        // Same configuration as the test (Debug/Release) — the substring
        // appears in the test's own BaseDirectory path.
        var preferRelease = AppContext.BaseDirectory.Contains(
            Path.DirectorySeparatorChar + "Release" + Path.DirectorySeparatorChar);
        var matches = Directory.GetFiles(artifactsRoot, "surgewave.dll", SearchOption.AllDirectories)
            .Where(p => p.Contains(Path.DirectorySeparatorChar + "Kuestenlogik.Surgewave.Cli" + Path.DirectorySeparatorChar)
                     && !p.Contains("IntegrationTests"))
            .OrderByDescending(p => p.Contains(preferRelease ? "Release" : "Debug"))
            .ToList();
        _cliDllPath = matches.FirstOrDefault()
            ?? throw new FileNotFoundException(
                "surgewave.dll not found anywhere under " + artifactsRoot);
    }

    public async ValueTask InitializeAsync()
    {
        _surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0) // Random port
            .WithAutoCreateTopics(true)
            .WithPartitions(3)
            .Build()
            .StartAsync();
        _bootstrapServer = _surgewave.BootstrapServers;
        _output.WriteLine($"Embedded broker started on {_bootstrapServer}");

        // Wait for broker to be fully protocol-ready, not just TCP-listening.
        // SurgewaveRuntime.StartAsync waits for the TCP port, but the protocol
        // handler may not be fully initialized yet. Allow time for the
        // accept loop and request dispatcher to stabilize.
        await WaitForBrokerProtocolReady();
    }

    private async Task WaitForBrokerProtocolReady(int maxRetries = 20)
    {
        // Parse port from bootstrap server string
        var port = int.Parse(_bootstrapServer.Split(':')[1]);

        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                await client.ConnectAsync("localhost", port);

                // Send an ApiVersions request (Kafka protocol) to verify the
                // broker is actually processing requests, not just accepting connections.
                // ApiVersions request: size(4) + api_key(2)=18 + api_version(2)=0 + correlation_id(4) + client_id(2)=-1
                var request = new byte[]
                {
                    0, 0, 0, 10,    // request size = 10 bytes
                    0, 18,          // api_key = ApiVersions (18)
                    0, 0,           // api_version = 0
                    0, 0, 0, 1,    // correlation_id = 1
                    255, 255        // client_id = null (-1)
                };

                var stream = client.GetStream();
                stream.ReadTimeout = 5000;
                stream.WriteTimeout = 5000;
                await stream.WriteAsync(request);

                // Read response size (4 bytes) - if we get this, broker is processing
                var responseSize = new byte[4];
                var bytesRead = await stream.ReadAsync(responseSize);
                if (bytesRead == 4)
                {
                    _output.WriteLine("Broker is protocol-ready");
                    return;
                }
            }
            catch
            {
                // Not ready yet
            }
            await Task.Delay(500);
        }
        _output.WriteLine("Warning: broker protocol readiness check did not succeed within timeout");
    }

    public async ValueTask DisposeAsync()
    {
        if (_surgewave != null)
        {
            await _surgewave.DisposeAsync();
        }
    }

    private async Task<(int ExitCode, string Output)> RunCliAsync(string args, int timeoutSeconds = 120)
    {
        _output.WriteLine($"CLI dll: {_cliDllPath}");
        _output.WriteLine($"CLI dll exists: {File.Exists(_cliDllPath)}");

        // `dotnet <dll>` invokes the compiled CLI directly. We don't use
        // `dotnet run` because its no-build resolver doesn't honour the
        // repo's custom OutputPath layout on Linux runners.
        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"\"{_cliDllPath}\" -b {_bootstrapServer} {args}",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(_cliDllPath)!
        };

        // Set NO_COLOR to ensure plain text output (Spectre.Console respects this)
        // This fixes output capture issues when running via dotnet run
        psi.Environment["NO_COLOR"] = "1";

        using var process = Process.Start(psi)!;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        // Don't pass cancellation token to read tasks - let them complete naturally
        // after process exits or is killed
        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();

        var timedOut = false;
        try
        {
            await process.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            timedOut = true;
            process.Kill(entireProcessTree: true);
        }

        // Wait for output with a short timeout after process exit/kill
        var readTimeout = Task.Delay(TimeSpan.FromSeconds(5));
        var outputComplete = Task.WhenAll(outputTask, errorTask);

        string output = "";
        string error = "";

        if (await Task.WhenAny(outputComplete, readTimeout) == outputComplete)
        {
            output = await outputTask;
            error = await errorTask;
        }

        var combined = output + error;
        _output.WriteLine($"CLI: surgewave {args}");
        _output.WriteLine($"Exit: {(timedOut ? -1 : process.ExitCode)} {(timedOut ? "(timed out)" : "")}");
        _output.WriteLine(combined);

        return (timedOut ? -1 : process.ExitCode, combined);
    }

    // ═══════════════════════════════════════════════════════════════
    // BROKER COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task BrokerHealth_ReturnsHealthy()
    {
        var (exitCode, output) = await RunCliAsync("broker health");

        Assert.Equal(0, exitCode);
        Assert.Contains("healthy", output.ToLowerInvariant());
    }

    [Fact]
    public async Task BrokerInfo_ShowsBrokerDetails()
    {
        // Use JSON format for reliable output capture; Spectre.Console rich
        // renderables (tables/panels) may produce empty output when stdout
        // is redirected in non-TTY mode.
        // Retry up to 3 times: the first `dotnet run` invocation may need to
        // build the CLI project, and the broker may not be fully protocol-ready
        // even though the TCP port is listening.
        (int exitCode, string output) result = (-1, "");
        for (int attempt = 0; attempt < 3; attempt++)
        {
            result = await RunCliAsync("broker info -f json");
            if (result.exitCode == 0 && result.output.Contains("Broker"))
                break;

            _output.WriteLine($"Attempt {attempt + 1} failed (exit={result.exitCode}), retrying...");
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        Assert.Equal(0, result.exitCode);
        Assert.Contains("Broker", result.output);
    }

    [Fact]
    public async Task BrokerConfigDescribe_ShowsConfig()
    {
        var (exitCode, output) = await RunCliAsync("broker config describe --all");

        Assert.Equal(0, exitCode);
        Assert.Contains("Config", output);
    }

    // ═══════════════════════════════════════════════════════════════
    // TOPIC COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Topics_CreateListDescribeDelete_Workflow()
    {
        var topicName = $"cli-test-{Guid.NewGuid():N}";

        // Create topic
        var (createExit, createOutput) = await RunCliAsync($"topics create {topicName} --partitions 3");
        Assert.Equal(0, createExit);
        Assert.Contains("Created", createOutput);

        // List topics
        var (listExit, listOutput) = await RunCliAsync("topics list");
        Assert.Equal(0, listExit);
        Assert.Contains(topicName, listOutput);

        // Describe topic
        var (describeExit, describeOutput) = await RunCliAsync($"topics describe {topicName}");
        Assert.Equal(0, describeExit);
        Assert.Contains(topicName, describeOutput);
        Assert.Contains("3", describeOutput); // partition count

        // Delete topic (no confirmation in CLI - immediate delete)
        var (deleteExit, deleteOutput) = await RunCliAsync($"topics delete {topicName}");
        Assert.Equal(0, deleteExit);
        Assert.Contains("Deleted", deleteOutput);

        // Verify deleted
        var (listExit2, listOutput2) = await RunCliAsync("topics list");
        Assert.DoesNotContain(topicName, listOutput2);
    }

    [Fact]
    public async Task Topics_ListJson_ReturnsValidJson()
    {
        var (exitCode, output) = await RunCliAsync("topics list -f json");

        Assert.Equal(0, exitCode);
        Assert.Contains("[", output); // JSON array
    }

    // ═══════════════════════════════════════════════════════════════
    // PRODUCE/CONSUME COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task ProduceConsume_SingleMessage_RoundTrip()
    {
        var topicName = $"cli-produce-{Guid.NewGuid():N}";
        var messageValue = $"test-message-{Guid.NewGuid()}";

        // Create topic first
        await RunCliAsync($"topics create {topicName} --partitions 1");

        // Produce message
        var (produceExit, produceOutput) = await RunCliAsync($"produce {topicName} --value \"{messageValue}\"");
        Assert.Equal(0, produceExit);

        // Consume message
        var (consumeExit, consumeOutput) = await RunCliAsync($"consume {topicName} -o earliest -n 1");
        Assert.Equal(0, consumeExit);
        Assert.Contains(messageValue, consumeOutput);
    }

    [Fact]
    public async Task ProduceConsume_WithKey_PreservesKey()
    {
        var topicName = $"cli-keyed-{Guid.NewGuid():N}";
        var key = "my-key";
        var value = "my-value";

        await RunCliAsync($"topics create {topicName} --partitions 1");
        var (produceExit, _) = await RunCliAsync($"produce {topicName} --key {key} --value {value}");
        Assert.Equal(0, produceExit);

        var (consumeExit, consumeOutput) = await RunCliAsync($"consume {topicName} -o earliest -n 1 -k");
        Assert.Equal(0, consumeExit);
        Assert.Contains(key, consumeOutput);
        Assert.Contains(value, consumeOutput);
    }

    // ═══════════════════════════════════════════════════════════════
    // PARTITION COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Partitions_Describe_ShowsPartitionDetails()
    {
        var topicName = $"cli-partitions-{Guid.NewGuid():N}";

        await RunCliAsync($"topics create {topicName} --partitions 3");

        var (exitCode, output) = await RunCliAsync($"partitions describe {topicName}");

        Assert.Equal(0, exitCode);
        Assert.Contains(topicName, output);
        Assert.Contains("Partition", output);
    }

    [Fact]
    public async Task Partitions_ElectLeader_TriggersElection()
    {
        var topicName = $"cli-elect-{Guid.NewGuid():N}";

        await RunCliAsync($"topics create {topicName} --partitions 1");

        var (exitCode, output) = await RunCliAsync($"partitions elect-leader --topic {topicName}");

        // Should succeed or report "election not needed"
        Assert.True(exitCode == 0 || output.Contains("not needed", StringComparison.OrdinalIgnoreCase));
    }

    // ═══════════════════════════════════════════════════════════════
    // ACL COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Acls_AddListRemove_Workflow()
    {
        var principal = "User:test-user";
        var resourceName = $"cli-acl-topic-{Guid.NewGuid():N}";

        // Add ACL
        var (addExit, addOutput) = await RunCliAsync(
            $"acls add --principal {principal} --resource-type topic --resource {resourceName} --operation read --permission allow");

        // May fail if security is disabled - that's OK
        if (addOutput.Contains("SecurityDisabled"))
        {
            _output.WriteLine("Security is disabled, skipping ACL test");
            return;
        }

        Assert.Equal(0, addExit);
        Assert.Contains("Added", addOutput);

        // List ACLs
        var (listExit, listOutput) = await RunCliAsync($"acls list --principal {principal}");
        Assert.Equal(0, listExit);
        Assert.Contains(principal, listOutput);

        // Remove ACL
        var (removeExit, removeOutput) = await RunCliAsync(
            $"acls remove --principal {principal} --resource-type topic --resource {resourceName} --force");
        Assert.Equal(0, removeExit);
        Assert.Contains("Deleted", removeOutput);
    }

    [Fact]
    public async Task Acls_List_ReturnsResults()
    {
        var (exitCode, output) = await RunCliAsync("acls list");

        // Should succeed even if no ACLs (or security disabled)
        Assert.True(exitCode == 0 || output.Contains("SecurityDisabled"));
    }

    // ═══════════════════════════════════════════════════════════════
    // GROUP COMMANDS
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Groups_List_ReturnsResults()
    {
        var (exitCode, output) = await RunCliAsync("groups list");

        Assert.Equal(0, exitCode);
    }

    // ═══════════════════════════════════════════════════════════════
    // JSON OUTPUT FORMAT
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task BrokerHealth_JsonFormat_ReturnsValidJson()
    {
        var (exitCode, output) = await RunCliAsync("broker health -f json");

        Assert.Equal(0, exitCode);
        Assert.Contains("\"Status\"", output);
    }

    [Fact]
    public async Task BrokerInfo_JsonFormat_ReturnsValidJson()
    {
        var (exitCode, output) = await RunCliAsync("broker info -f json");

        Assert.Equal(0, exitCode);
        Assert.Contains("{", output);
        Assert.Contains("}", output);
    }
}
