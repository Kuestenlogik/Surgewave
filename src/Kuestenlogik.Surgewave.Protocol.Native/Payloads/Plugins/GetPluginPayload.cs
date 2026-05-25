namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for GetPlugin request.
/// </summary>
public readonly record struct GetPluginRequestPayload
{
    public string PackageId { get; init; }
    public string? Version { get; init; }

    public static GetPluginRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new GetPluginRequestPayload
        {
            PackageId = reader.ReadString() ?? string.Empty,
            Version = reader.ReadNullableString()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteNullableString(Version);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteNullableString(Version);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "")
             + 1 + (Version != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(Version) : 0);
    }
}

/// <summary>
/// Wire format for GetPlugin response.
/// </summary>
public readonly record struct GetPluginResponsePayload
{
    public bool Found { get; init; }
    public PluginInfoPayload? Plugin { get; init; }

    public static GetPluginResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var found = reader.ReadBoolean();
        PluginInfoPayload? plugin = null;
        if (found)
        {
            plugin = PluginInfoPayload.Read(ref reader);
        }

        return new GetPluginResponsePayload
        {
            Found = found,
            Plugin = plugin
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteBoolean(Found);
        if (Found && Plugin.HasValue)
        {
            Plugin.Value.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteBoolean(Found);
        if (Found && Plugin.HasValue)
        {
            Plugin.Value.WriteTo(writer);
        }
    }

    public int EstimateSize()
    {
        int size = 1; // Found
        if (Found && Plugin.HasValue)
        {
            size += Plugin.Value.EstimateSize();
        }
        return size;
    }
}
