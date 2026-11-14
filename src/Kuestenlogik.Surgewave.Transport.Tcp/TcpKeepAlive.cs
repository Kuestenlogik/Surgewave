using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.Transport.Tcp;

/// <summary>
/// Has the operating system probe a connection that carries nothing, so that a broker that vanished is noticed (#289).
/// </summary>
/// <remarks>
/// A broker that closes or resets its connection says so, and the read that waits for its answer fails. A host that
/// loses power, or a network that drops its packets, says nothing: the read waits for as long as the client runs.
/// With keep-alive the operating system asks the idle peer for a sign of life after
/// <see cref="TransportOptions.KeepAliveTime"/>, again every <see cref="TransportOptions.KeepAliveInterval"/>, and
/// fails the connection — and the read with it — once <see cref="TransportOptions.KeepAliveRetryCount"/> of them
/// went unanswered.
/// </remarks>
internal static class TcpKeepAlive
{
    /// <summary>
    /// Turns keep-alive on for <paramref name="socket"/> as <paramref name="options"/> say, or leaves it off where
    /// <see cref="TransportOptions.KeepAliveTime"/> is <see cref="Timeout.InfiniteTimeSpan"/>.
    /// </summary>
    public static void Apply(Socket socket, TransportOptions options)
    {
        if (options.KeepAliveTime == Timeout.InfiniteTimeSpan)
            return;

        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, Seconds(options.KeepAliveTime));
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, Seconds(options.KeepAliveInterval));
        socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, options.KeepAliveRetryCount);
    }

    // The options take whole seconds, and none of them takes zero.
    private static int Seconds(TimeSpan time) => Math.Max(1, (int)Math.Ceiling(time.TotalSeconds));
}
