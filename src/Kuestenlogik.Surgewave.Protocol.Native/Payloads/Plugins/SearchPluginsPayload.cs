namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for SearchPlugins request.
/// </summary>
public readonly record struct SearchPluginsRequestPayload
{
    public string Query { get; init; }
    public int Skip { get; init; }
    public int Take { get; init; }

    public static SearchPluginsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new SearchPluginsRequestPayload
        {
            Query = reader.ReadString() ?? string.Empty,
            Skip = reader.ReadInt32(),
            Take = reader.ReadInt32()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Query);
        writer.WriteInt32(Skip);
        writer.WriteInt32(Take);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Query);
        writer.WriteInt32(Skip);
        writer.WriteInt32(Take);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(Query ?? "")
             + 4 + 4;
    }
}

/// <summary>
/// Wire format for SearchPlugins response.
/// </summary>
public readonly record struct SearchPluginsResponsePayload
{
    public IReadOnlyList<PluginInfoPayload> Plugins { get; init; }
    public int TotalCount { get; init; }

    public static SearchPluginsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var plugins = new List<PluginInfoPayload>(count);
        for (int i = 0; i < count; i++)
        {
            plugins.Add(PluginInfoPayload.Read(ref reader));
        }

        return new SearchPluginsResponsePayload
        {
            Plugins = plugins,
            TotalCount = reader.ReadInt32()
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
        writer.WriteInt32(TotalCount);
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
        writer.WriteInt32(TotalCount);
    }

    public int EstimateSize()
    {
        int size = 4 + 4; // count + totalCount
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
