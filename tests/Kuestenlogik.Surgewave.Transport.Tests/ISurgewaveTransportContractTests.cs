using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Contract tests for ISurgewaveTransport implementations using a mock.
/// Verifies that the interface contract is well-defined.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ISurgewaveTransportContractTests
{
    [Fact]
    public void ISurgewaveTransport_HasTransportType_Property()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        transport.TransportType.Returns(SurgewaveTransportType.Tcp);

        // Act & Assert
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public void ISurgewaveTransport_IsConnected_CanBeTrue()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        transport.IsConnected.Returns(true);

        // Act & Assert
        Assert.True(transport.IsConnected);
    }

    [Fact]
    public void ISurgewaveTransport_IsConnected_CanBeFalse()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        transport.IsConnected.Returns(false);

        // Act & Assert
        Assert.False(transport.IsConnected);
    }

    [Fact]
    public void ISurgewaveTransport_ServerSupportsCompression_ReflectsCapability()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        transport.ServerSupportsCompression.Returns(true);

        // Act & Assert
        Assert.True(transport.ServerSupportsCompression);
    }

    [Fact]
    public async Task ISurgewaveTransport_ConnectAsync_IsCalled()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        transport.ConnectAsync(Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);

        // Act
        await transport.ConnectAsync();

        // Assert
        await transport.Received(1).ConnectAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ISurgewaveTransport_SendRequestAsync_ReturnsHeaderAndPayload()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        var expectedHeader = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 1u,
            OpCode = SurgewaveOpCode.ProduceAck,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 3
        };
        var expectedPayload = new byte[] { 1, 2, 3 };

        transport.SendRequestAsync(
            Arg.Any<SurgewaveOpCode>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<bool>(),
            Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult<(SurgewaveResponseHeader, ReadOnlyMemory<byte>)>((expectedHeader, expectedPayload)));

        // Act
        var (header, payload) = await transport.SendRequestAsync(
            SurgewaveOpCode.Produce,
            new byte[] { 0xAA }.AsMemory());

        // Assert
        Assert.Equal(expectedHeader.RequestId, header.RequestId);
        Assert.Equal(expectedPayload, payload.ToArray());
    }

    [Fact]
    public async Task ISurgewaveTransport_DisposeAsync_IsCalled()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();

        // Act
        await using (transport)
        {
            // just use the transport
        }

        // Assert
        await transport.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task ISurgewaveTransport_SendRequestAsync_WithCancellation_IsCancellable()
    {
        // Arrange
        var transport = Substitute.For<ISurgewaveTransport>();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        transport.SendRequestAsync(
            Arg.Any<SurgewaveOpCode>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<bool>(),
            Arg.Is<CancellationToken>(t => t.IsCancellationRequested))
            .Returns(ValueTask.FromException<(SurgewaveResponseHeader, ReadOnlyMemory<byte>)>(
                new OperationCanceledException()));

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await transport.SendRequestAsync(SurgewaveOpCode.Produce, ReadOnlyMemory<byte>.Empty,
                cancellationToken: cts.Token));
    }
}
