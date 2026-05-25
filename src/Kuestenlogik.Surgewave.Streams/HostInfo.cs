namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Host and port of a Streams application instance for Remote Interactive Queries.
/// </summary>
/// <param name="Host">The hostname or IP address.</param>
/// <param name="Port">The port number.</param>
public readonly record struct HostInfo(string Host, int Port)
{
    /// <inheritdoc />
    public override string ToString() => $"{Host}:{Port}";

    /// <summary>Parses a "host:port" string into a <see cref="HostInfo"/>.</summary>
    /// <param name="hostPort">The string to parse in "host:port" format.</param>
    /// <returns>A parsed <see cref="HostInfo"/> instance.</returns>
    public static HostInfo Parse(string hostPort)
    {
        var parts = hostPort.Split(':');
        return new HostInfo(parts[0], int.Parse(parts[1]));
    }
}
