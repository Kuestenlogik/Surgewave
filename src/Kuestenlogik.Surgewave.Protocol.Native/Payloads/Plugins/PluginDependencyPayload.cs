namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for plugin dependency information.
/// </summary>
public readonly record struct PluginDependencyPayload
{
    public string Id { get; init; }
    public string Version { get; init; }
    public bool Optional { get; init; }

    public static PluginDependencyPayload Read(ref SurgewavePayloadReader reader)
    {
        return new PluginDependencyPayload
        {
            Id = reader.ReadString() ?? string.Empty,
            Version = reader.ReadString() ?? "*",
            Optional = reader.ReadBoolean()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Id);
        writer.WriteString(Version);
        writer.WriteBoolean(Optional);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Id);
        writer.WriteString(Version);
        writer.WriteBoolean(Optional);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(Id ?? "")
             + 2 + System.Text.Encoding.UTF8.GetByteCount(Version ?? "")
             + 1; // boolean
    }
}
