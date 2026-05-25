using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Checks whether the cloud broker is reachable via TCP connection.
/// Used by <see cref="EdgeSyncService"/> to determine online/offline state
/// before attempting message synchronization.
/// </summary>
public sealed class ConnectivityChecker
{
    private readonly ILogger<ConnectivityChecker> _logger;

    /// <summary>
    /// Creates a new connectivity checker.
    /// </summary>
    public ConnectivityChecker(ILogger<ConnectivityChecker> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Attempts a TCP connection to the cloud broker to check reachability.
    /// </summary>
    /// <param name="address">The broker address in <c>host:port</c> format.</param>
    /// <param name="timeoutMs">Connection timeout in milliseconds.</param>
    /// <returns>True if the broker is reachable within the timeout.</returns>
    public async Task<bool> IsCloudReachableAsync(string address, int timeoutMs = 5000)
    {
        var (host, port) = ParseAddress(address);

        try
        {
            using var client = new TcpClient();
            using var cts = new CancellationTokenSource(timeoutMs);
            await client.ConnectAsync(host, port, cts.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Cloud broker connectivity check timed out after {TimeoutMs}ms: {Address}", timeoutMs, address);
            return false;
        }
        catch (SocketException ex)
        {
            _logger.LogDebug("Cloud broker not reachable at {Address}: {Error}", address, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Parses a <c>host:port</c> address string into its components.
    /// </summary>
    /// <param name="address">Address in <c>host:port</c> format.</param>
    /// <returns>A tuple of host and port.</returns>
    /// <exception cref="ArgumentException">Thrown if the address format is invalid.</exception>
    public static (string Host, int Port) ParseAddress(string address)
    {
        var colonIndex = address.LastIndexOf(':');
        if (colonIndex < 0 || colonIndex == address.Length - 1)
        {
            throw new ArgumentException($"Invalid broker address format: '{address}'. Expected 'host:port'.", nameof(address));
        }

        var host = address[..colonIndex];
        if (!int.TryParse(address.AsSpan(colonIndex + 1), out var port) || port is < 1 or > 65535)
        {
            throw new ArgumentException($"Invalid port in broker address: '{address}'.", nameof(address));
        }

        return (host, port);
    }
}
