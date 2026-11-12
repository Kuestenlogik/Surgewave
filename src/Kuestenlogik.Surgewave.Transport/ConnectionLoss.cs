using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Says why a connection to a broker ended, in the words a caller whose request was waiting on it can act on (#281).
/// </summary>
/// <remarks>
/// <para>
/// A request that waits for its answer when the connection goes is failed with what the read of the next answer
/// threw: "Unable to read beyond the end of the stream" when the broker closed the connection, "Stream aborted by
/// peer (1)" over QUIC. Neither names a connection or a broker. The failure built here does, and keeps what was
/// thrown as its inner exception.
/// </para>
/// <para>
/// It is an <see cref="IOException"/> whatever ended the connection, because that is what the client's retry
/// policies and the consumer's reconnect take for a lost connection. Shared by the transports so that TCP and QUIC
/// say the same thing.
/// </para>
/// </remarks>
public static class ConnectionLoss
{
    /// <summary>The failure for requests on a connection to <paramref name="host"/>:<paramref name="port"/> that ended with <paramref name="cause"/>.</summary>
    public static IOException Describe(Exception cause, string host, int port)
    {
        ArgumentNullException.ThrowIfNull(cause);

        var message = cause switch
        {
            EndOfStreamException => $"The broker at {host}:{port} closed the connection.",
            _ when IsReset(cause) => $"The connection to the broker at {host}:{port} was reset: the broker dropped it or is gone.",
            _ => $"The connection to the broker at {host}:{port} ended: {cause.Message}",
        };

        return new IOException(message, cause);
    }

    /// <summary>
    /// The failure for a request made on a connection that ended earlier with <paramref name="ended"/>: a new
    /// exception with the same words, so that one request's stack trace is not written over another's.
    /// </summary>
    public static Exception Again(Exception ended)
    {
        ArgumentNullException.ThrowIfNull(ended);
        return ended is ObjectDisposedException disposed
            ? new ObjectDisposedException(disposed.ObjectName)
            : new IOException(ended.Message, ended.InnerException ?? ended);
    }

    private static bool IsReset(Exception cause)
    {
        for (var e = cause; e is not null; e = e.InnerException)
        {
            if (e is SocketException { SocketErrorCode: SocketError.ConnectionReset or SocketError.ConnectionAborted or SocketError.Shutdown })
                return true;
        }

        return false;
    }
}
