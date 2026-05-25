namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for ListInstalledPlugins request.
/// Empty request - no parameters needed.
/// </summary>
public readonly record struct ListInstalledPluginsRequestPayload
{
    public static ListInstalledPluginsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new ListInstalledPluginsRequestPayload();
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        // No data to write
    }

    public void WriteTo(IPayloadWriter writer)
    {
        // No data to write
    }

    public int EstimateSize() => 0;
}

/// <summary>
/// Wire format for ListInstalledPlugins response.
/// </summary>
public readonly record struct ListInstalledPluginsResponsePayload
{
    public IReadOnlyList<PluginInfoPayload> Plugins { get; init; }

    public static ListInstalledPluginsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var plugins = new List<PluginInfoPayload>(count);
        for (int i = 0; i < count; i++)
        {
            plugins.Add(PluginInfoPayload.Read(ref reader));
        }

        return new ListInstalledPluginsResponsePayload
        {
            Plugins = plugins
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Plugins?.Count ?? 0);
        if (Plugins != null)
        {
            foreach (var plugin in Plugins)
            {
                plugin.Write(ref writer);
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Plugins?.Count ?? 0);
        if (Plugins != null)
        {
            foreach (var plugin in Plugins)
            {
                plugin.WriteTo(writer);
            }
        }
    }

    public int EstimateSize()
    {
        int size = 4;
        if (Plugins != null)
        {
            foreach (var plugin in Plugins)
            {
                size += plugin.EstimateSize();
            }
        }
        return size;
    }
}
