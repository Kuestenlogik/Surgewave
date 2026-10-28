using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// A broker a Java Kafka client can reach, and that client: Apache Kafka's own command-line tools in an
/// apache/kafka container (#195). The broker advertises the address it binds, so it binds the host's outbound
/// IPv4 address — loopback inside a container is the container. The container only sleeps; each tool runs
/// in it through <see cref="RunAsync"/>. Without Docker the fixture records why, and every test skips.
/// </summary>
public sealed class JavaClientFixture : IAsyncLifetime
{
    /// <summary>The Java client under test: the newest Kafka the broker targets.</summary>
    private const string ClientImage = "apache/kafka:4.1.0";

    private readonly ILoggerFactory _loggerFactory = TestLogging.CreateConsoleFactory();
    private SurgewaveRuntime? _broker;
    private IContainer? _client;

    /// <summary>The broker's address as the Java client reaches it.</summary>
    public string BootstrapServers { get; private set; } = string.Empty;

    /// <summary>Why the Java client could not be started, or null when it runs.</summary>
    public string? Unavailable { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var host = OutboundIPv4Address();
        if (host is null)
        {
            Unavailable = "the host has no IPv4 address a container can reach";
            return;
        }

        try
        {
            _client = new ContainerBuilder(ClientImage)
                .WithEntrypoint("sleep")
                .WithCommand("infinity")
                .Build();
            await _client.StartAsync();
        }
        catch (Exception ex) when (ex is DockerUnavailableException or TimeoutException or HttpRequestException)
        {
            Unavailable = $"Docker is not available ({ex.GetType().Name}: {ex.Message})";
            return;
        }

        _broker = await SurgewaveRuntime.CreateBuilder()
            .WithHost(host.ToString())
            .WithPort(0)
            .WithPartitions(3)
            .WithAutoCreateTopics(true)
            .WithShutdownTimeout(5)
            .WithLogging(_loggerFactory)
            .Build()
            .StartAsync();
        BootstrapServers = _broker.BootstrapServers;
    }

    /// <summary>Skips the calling test when the Java client is not available.</summary>
    public void SkipUnlessAvailable()
    {
        if (Unavailable is not null)
            Assert.Skip($"Java client E2E skipped: {Unavailable}");
    }

    /// <summary>
    /// Runs a shell command in the client container, with the Kafka tools on the path and
    /// <c>$BOOTSTRAP</c> set to the broker.
    /// </summary>
    public async Task<ExecResult> RunAsync(string command, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("The Java client is not running.");
        return await client.ExecAsync(
            ["sh", "-c", $"export PATH=/opt/kafka/bin:$PATH BOOTSTRAP={BootstrapServers}; {command}"],
            cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync();
        if (_broker is not null)
            await _broker.DisposeAsync();
        _loggerFactory.Dispose();
    }

    /// <summary>
    /// The address of the interface the host routes outbound traffic through. Connecting a UDP socket sends
    /// nothing; it only selects the route. Without a route, the first up IPv4 interface that is not loopback.
    /// </summary>
    private static IPAddress? OutboundIPv4Address()
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9));
            if (socket.LocalEndPoint is IPEndPoint { Address: var address } && !IPAddress.IsLoopback(address))
                return address;
        }
        catch (SocketException)
        {
            // No route: fall back to the interfaces.
        }

        return NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Select(a => a.Address)
            .FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork);
    }
}

/// <summary>One broker and one Java client for every Java client test class.</summary>
[CollectionDefinition("JavaClient")]
public sealed class JavaClientCollection : ICollectionFixture<JavaClientFixture>;
