using Kuestenlogik.Surgewave.Protocol;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for <see cref="ISurgewaveStreamHandler"/> — the magic-byte detection
/// that routes SRWV-prefixed streams to the native handler and everything
/// else to the Kafka handler.
/// </summary>
public class SurgewaveStreamHandlerTests
{
    [Fact]
    public async Task HandleAsync_StreamShorterThan4Bytes_GracefulReturn()
    {
        // A stream that yields only 2 bytes then EOF should not throw — the
        // handler should return gracefully (no protocol detected).
        await using var runtime = await Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithStorageEngine(Kuestenlogik.Surgewave.Core.Storage.StorageEngines.Memory)
            .Build()
            .StartAsync();

        var handler = (ISurgewaveStreamHandler)runtime.Broker;

        // Feed a 2-byte stream that signals EOF after 2 bytes
        using var ms = new MemoryStream([0x00, 0x01]);

        // Should complete without throwing
        await handler.HandleAsync(ms, "test", null, CancellationToken.None);
    }

    [Fact]
    public async Task HandleAsync_KafkaMagicBytes_ProcessesWithoutCrash()
    {
        // Kafka requests start with a 4-byte size prefix (big-endian int32).
        // Feeding an invalid "Kafka" frame should not crash the handler — it
        // should either parse-fail gracefully or return after the connection drops.
        await using var runtime = await Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithStorageEngine(Kuestenlogik.Surgewave.Core.Storage.StorageEngines.Memory)
            .Build()
            .StartAsync();

        var handler = (ISurgewaveStreamHandler)runtime.Broker;

        // 4 non-SRWV bytes followed by EOF → Kafka path reads size, finds
        // invalid frame, logs warning, returns.
        var fakeKafka = new byte[] { 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00 };
        using var ms = new MemoryStream(fakeKafka);

        // Should return without throwing (broken frame logged internally)
        await handler.HandleAsync(ms, "test", null, CancellationToken.None);
    }

    [Fact]
    public async Task HandleAsync_SrwvMagic_RoutesToNativeHandler()
    {
        await using var runtime = await Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithStorageEngine(Kuestenlogik.Surgewave.Core.Storage.StorageEngines.Memory)
            .Build()
            .StartAsync();

        // Verify SRWV routing via a real loopback TCP connection.
        // The native handler needs a real network stream for its internal
        // PipeWriter — MemoryStream doesn't support CompleteAsync correctly.
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        using var client = new System.Net.Sockets.TcpClient();
        await client.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        listener.Stop();

        // Write SRWV magic + version byte from client side
        var clientStream = client.GetStream();
        await clientStream.WriteAsync(new byte[] { 0x53, 0x52, 0x57, 0x56, 0x01 });
        await clientStream.FlushAsync();
        client.Client.Shutdown(System.Net.Sockets.SocketShutdown.Send);

        var handler = (ISurgewaveStreamHandler)runtime.Broker;
        var serverStream = accepted.GetStream();

        // HandleAsync routes to native handler, which processes the
        // handshake (version byte) and returns when the client EOF arrives.
        await handler.HandleAsync(serverStream, "test", null, CancellationToken.None);
    }
}
