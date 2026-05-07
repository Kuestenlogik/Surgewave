namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Notification payload sent from broker to workers when a plugin is installed.
/// Workers use the plugin ID and version to pull the package via REST.
/// </summary>
public readonly record struct PushPluginNotificationPayload
{
    public string PluginId { get; init; }
    public string Version { get; init; }
    public string Sha256 { get; init; }

    public static PushPluginNotificationPayload Read(ref SurgewavePayloadReader reader)
    {
        return new PushPluginNotificationPayload
        {
            PluginId = reader.ReadString() ?? string.Empty,
            Version = reader.ReadString() ?? string.Empty,
            Sha256 = reader.ReadString() ?? string.Empty
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PluginId);
        writer.WriteString(Version);
        writer.WriteString(Sha256);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PluginId);
        writer.WriteString(Version);
        writer.WriteString(Sha256);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(PluginId ?? "")
             + 2 + System.Text.Encoding.UTF8.GetByteCount(Version ?? "")
             + 2 + System.Text.Encoding.UTF8.GetByteCount(Sha256 ?? "");
    }
}
