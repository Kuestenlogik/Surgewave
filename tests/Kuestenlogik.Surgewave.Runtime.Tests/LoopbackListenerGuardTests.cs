using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Runtime.Tests;

/// <summary>
/// A test listens on loopback and nothing else (#285). This reads the sources of every test project and of the test
/// helper projects and fails on a wildcard address — every interface — that is not listed here with its reason.
/// </summary>
/// <remarks>
/// On Windows a process that listens on every interface raises a firewall prompt per executable path, and each
/// worktree and each clean clone is a new path: a test run then waits for a person. Loopback is never filtered.
/// A test that something outside the host has to reach — a client in a container — belongs to the integration
/// tier, and only there may a listed entry say so; see docs/testing.md.
/// The guard reads what the tests write. A listener a product type binds by its own default is not in the test's
/// source: <see cref="EmbeddedBrokerListenerTests"/> holds that for the embedded broker, the helper most tests use, and for its replication listener.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed partial class LoopbackListenerGuardTests
{
    /// <summary>Why a source file may name a wildcard address.</summary>
    private enum Reason
    {
        /// <summary>The wildcard is a value the test reads or compares; nothing binds it.</summary>
        NeverBound,

        /// <summary>The listener has to be reached from outside the host. Integration tier only.</summary>
        ReachedFromOutsideTheHost,
    }

    /// <summary>
    /// The files that may name a wildcard address, each with what it is there for. A file that no longer names one
    /// has to leave the list.
    /// </summary>
    private static readonly Dictionary<string, (Reason Reason, string Why)> Listed = new(StringComparer.Ordinal)
    {
        ["tests/Kuestenlogik.Surgewave.Broker.Tests/BindAddressTests.cs"] =
            (Reason.NeverBound, "validates BrokerConfig.BindAddress values; no listener is created"),
        ["tests/Kuestenlogik.Surgewave.Broker.Tests/BrokerEndpointsTests.cs"] =
            (Reason.NeverBound, "rewrites Kestrel endpoint URLs in a configuration; no host is started"),
        ["tests/Kuestenlogik.Surgewave.Broker.Tests/HealthProbeTests.cs"] =
            (Reason.NeverBound, "turns configured wildcard URLs into the loopback URL the probe asks; its one listener is on loopback"),
        ["tests/Kuestenlogik.Surgewave.Broker.Tests/LocalListenerAddressTests.cs"] =
            (Reason.NeverBound, "computes BrokerConfig.LocalListenerAddress for wildcard values; no listener is created"),
        ["tests/Kuestenlogik.Surgewave.Clustering.Tests/ReplicationBindAddressTests.cs"] =
            (Reason.NeverBound, "asserts the endpoint an unset ReplicationBindAddress yields; the listener it starts is on loopback"),
        ["tests/Kuestenlogik.Surgewave.Edge.Tests/EdgeConfigTests.cs"] =
            (Reason.NeverBound, "chains builder calls; the broker is not started"),
        ["tests/Kuestenlogik.Surgewave.Runtime.Tests/LoopbackListenerGuardTests.cs"] =
            (Reason.NeverBound, "the sample lines this guard's own detection is tested with"),
        ["tests/Kuestenlogik.Surgewave.Runtime.Tests/SurgewaveRuntimeTests.cs"] =
            (Reason.NeverBound, "builds runtime options with a wildcard host and with dual mode; no runtime is started"),
        ["tests/Kuestenlogik.Surgewave.Transport.Tests/LossyUdpProxyTests.cs"] =
            (Reason.NeverBound, "the endpoint ReceiveFromAsync fills with the sender; the sockets are bound to loopback"),
        ["src/Kuestenlogik.Surgewave.Testing.Network/LossyUdpProxy.cs"] =
            (Reason.NeverBound, "the endpoint ReceiveFromAsync fills with the sender; the sockets are bound to loopback"),
    };

    /// <summary>The test projects in which a listener may have to be reached from outside the host.</summary>
    private static readonly string[] IntegrationTier =
    [
        "tests/Kuestenlogik.Surgewave.IntegrationTests/",
    ];

    [GeneratedRegex(@"\bIPAddress\.(IPv6)?Any\b")]
    private static partial Regex WildcardAddressMember();

    // "0.0.0.0", "::" and "[::]" as a string or inside one, e.g. "http://0.0.0.0:9095" or "--Host=::".
    [GeneratedRegex("""(?<![\d.])0\.0\.0\.0(?![\d.])|"::"|\[::\]|=::"|=::\s""")]
    private static partial Regex WildcardAddressLiteral();

    // Kestrel's and HttpListener's "every host": http://*:9093, https://+:9443.
    [GeneratedRegex(@"://[*+][:/""]")]
    private static partial Regex WildcardUrl();

    // Dual mode is IPv6Any with IPv4 mapped in: Socket.DualMode, BrokerConfig.EnableDualMode, WithDualMode().
    [GeneratedRegex(@"DualMode\s*=\s*true\b|\bWithDualMode\(\s*(true\s*)?\)")]
    private static partial Regex DualModeOn();

    [Fact]
    public void NoTestOrTestHelperNamesAWildcardAddressItIsNotListedFor()
    {
        var root = RepositoryRoot();
        var findings = new List<string>();
        var filesWithAWildcard = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in SourcesToRead(root))
        {
            var relative = Path.GetRelativePath(root, file).Replace('\\', '/');
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                var wildcard = WildcardIn(lines[i]);
                if (wildcard is null)
                    continue;
                filesWithAWildcard.Add(relative);
                if (!Listed.ContainsKey(relative))
                    findings.Add($"{relative}:{i + 1}: {wildcard} — {lines[i].Trim()}");
            }
        }

        Assert.True(findings.Count == 0,
            "A test listens on loopback only (127.0.0.1 / ::1); see docs/testing.md, 'Test tiers: what a test may listen on'. "
            + "These lines name a wildcard address:\n  " + string.Join("\n  ", findings)
            + "\nBind loopback. If the wildcard is only a value that nothing binds, or the test is in the integration "
            + $"tier and has to be reached from outside the host, list the file in {nameof(LoopbackListenerGuardTests)} "
            + "with the reason.");

        var stale = Listed.Keys.Where(path => !filesWithAWildcard.Contains(path)).ToList();
        Assert.True(stale.Count == 0,
            $"Listed in {nameof(LoopbackListenerGuardTests)} but naming no wildcard address any more — remove the "
            + "entry:\n  " + string.Join("\n  ", stale));
    }

    [Fact]
    public void OnlyTheIntegrationTierListsAListenerReachedFromOutsideTheHost()
    {
        var misplaced = Listed
            .Where(entry => entry.Value.Reason == Reason.ReachedFromOutsideTheHost
                && !IntegrationTier.Any(project => entry.Key.StartsWith(project, StringComparison.Ordinal)))
            .Select(entry => entry.Key)
            .ToList();

        Assert.True(misplaced.Count == 0,
            "A test that has to be reached from outside the host is an integration test; move it to "
            + string.Join(" or ", IntegrationTier) + ":\n  " + string.Join("\n  ", misplaced));
    }

    [Theory]
    [InlineData("var listener = new TcpListener(IPAddress.Any, 0);")]
    [InlineData("var listener = new TcpListener(System.Net.IPAddress.IPv6Any, 0);")]
    [InlineData("Host = \"0.0.0.0\",")]
    [InlineData("BindAddress = \"::\",")]
    [InlineData("\"--Surgewave:BindAddress=::\",")]
    [InlineData("app.Urls.Add(\"http://[::]:5000\");")]
    [InlineData("$\"--Kestrel:Endpoints:Grpc:Url=http://*:{port}\",")]
    [InlineData("listener.Prefixes.Add($\"http://+:{port}/\");")]
    [InlineData("listener.Server.DualMode = true;")]
    [InlineData("var config = new BrokerConfig { EnableDualMode = true };")]
    [InlineData("SurgewaveRuntime.CreateBuilder().WithDualMode().Build();")]
    public void AWildcardAddressIsFound(string line)
    {
        Assert.NotNull(WildcardIn(line));
    }

    [Theory]
    [InlineData("var listener = new TcpListener(IPAddress.Loopback, 0);")]
    [InlineData("var listener = new TcpListener(IPAddress.IPv6Loopback, 0);")]
    [InlineData("Host = \"127.0.0.1\",")]
    [InlineData("Host = \"10.0.0.1\",")]
    [InlineData("$\"--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:{port}\",")]
    [InlineData("listener.Prefixes.Add($\"http://localhost:{port}/\");")]
    [InlineData("var config = new BrokerConfig { EnableDualMode = false };")]
    [InlineData("SurgewaveRuntime.CreateBuilder().WithDualMode(false).Build();")]
    [InlineData("Assert.False(options.EnableDualMode);")]
    [InlineData("// binds 0.0.0.0 in production, which a test never does")]
    [InlineData("/// The broker listens on <c>0.0.0.0</c> there.")]
    public void WhatIsNoWildcardBindIsNotFound(string line)
    {
        Assert.Null(WildcardIn(line));
    }

    /// <summary>The wildcard a line of code names, or null. A line that is only a comment names none.</summary>
    private static string? WildcardIn(string line)
    {
        var code = line.TrimStart();
        if (code.StartsWith("//", StringComparison.Ordinal) || code.StartsWith('*') || code.StartsWith("/*", StringComparison.Ordinal))
            return null;

        foreach (var pattern in (Regex[])[WildcardAddressMember(), WildcardAddressLiteral(), WildcardUrl(), DualModeOn()])
        {
            var match = pattern.Match(code);
            if (match.Success)
                return $"'{match.Value}'";
        }
        return null;
    }

    private static IEnumerable<string> SourcesToRead(string root)
    {
        var directories = new List<string> { Path.Combine(root, "tests") };
        directories.AddRange(Directory.GetDirectories(Path.Combine(root, "src"), "Kuestenlogik.Surgewave.Testing*"));

        var separator = Path.DirectorySeparatorChar;
        return directories
            .SelectMany(directory => Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
            .Where(file => !file.Contains($"{separator}bin{separator}", StringComparison.Ordinal)
                && !file.Contains($"{separator}obj{separator}", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal);
    }

    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Kuestenlogik.Surgewave.slnx")))
                return directory.FullName;
        }
        throw new DirectoryNotFoundException(
            $"Kuestenlogik.Surgewave.slnx was not found above {AppContext.BaseDirectory}: the guard reads the test sources of the checkout it was built from.");
    }
}
