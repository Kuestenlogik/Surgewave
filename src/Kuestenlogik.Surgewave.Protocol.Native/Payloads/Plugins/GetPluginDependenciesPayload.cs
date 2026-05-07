namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for GetPluginDependencies request.
/// </summary>
public readonly record struct GetPluginDependenciesRequestPayload
{
    public string PackageId { get; init; }
    public string? Version { get; init; }

    public static GetPluginDependenciesRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new GetPluginDependenciesRequestPayload
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
/// A node in the dependency tree.
/// </summary>
public readonly record struct DependencyTreeNodePayload
{
    public string PackageId { get; init; }
    public string Version { get; init; }
    public bool IsInstalled { get; init; }
    public bool IsMissing { get; init; }
    public bool IsCircular { get; init; }
    public IReadOnlyList<DependencyTreeNodePayload> Children { get; init; }

    public static DependencyTreeNodePayload Read(ref SurgewavePayloadReader reader)
    {
        var packageId = reader.ReadString() ?? string.Empty;
        var version = reader.ReadString() ?? string.Empty;
        var isInstalled = reader.ReadBoolean();
        var isMissing = reader.ReadBoolean();
        var isCircular = reader.ReadBoolean();

        // Read children (recursive)
        var childCount = reader.ReadInt32();
        var children = new List<DependencyTreeNodePayload>(childCount);
        for (int i = 0; i < childCount; i++)
        {
            children.Add(Read(ref reader));
        }

        return new DependencyTreeNodePayload
        {
            PackageId = packageId,
            Version = version,
            IsInstalled = isInstalled,
            IsMissing = isMissing,
            IsCircular = isCircular,
            Children = children
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Version);
        writer.WriteBoolean(IsInstalled);
        writer.WriteBoolean(IsMissing);
        writer.WriteBoolean(IsCircular);

        // Write children (recursive)
        writer.WriteInt32(Children?.Count ?? 0);
        if (Children != null)
        {
            foreach (var child in Children)
            {
                child.Write(ref writer);
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Version);
        writer.WriteBoolean(IsInstalled);
        writer.WriteBoolean(IsMissing);
        writer.WriteBoolean(IsCircular);

        // Write children (recursive)
        writer.WriteInt32(Children?.Count ?? 0);
        if (Children != null)
        {
            foreach (var child in Children)
            {
                child.WriteTo(writer);
            }
        }
    }

    public int EstimateSize()
    {
        int size = 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "")
                 + 2 + System.Text.Encoding.UTF8.GetByteCount(Version ?? "")
                 + 1 + 1 + 1 // bools
                 + 4; // child count

        if (Children != null)
        {
            foreach (var child in Children)
            {
                size += child.EstimateSize();
            }
        }

        return size;
    }
}

/// <summary>
/// Wire format for GetPluginDependencies response.
/// </summary>
public readonly record struct GetPluginDependenciesResponsePayload
{
    public bool Found { get; init; }
    public DependencyTreeNodePayload? Root { get; init; }

    public static GetPluginDependenciesResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var found = reader.ReadBoolean();
        DependencyTreeNodePayload? root = null;
        if (found)
        {
            root = DependencyTreeNodePayload.Read(ref reader);
        }

        return new GetPluginDependenciesResponsePayload
        {
            Found = found,
            Root = root
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteBoolean(Found);
        if (Found && Root.HasValue)
        {
            Root.Value.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteBoolean(Found);
        if (Found && Root.HasValue)
        {
            Root.Value.WriteTo(writer);
        }
    }

    public int EstimateSize()
    {
        int size = 1;
        if (Found && Root.HasValue)
        {
            size += Root.Value.EstimateSize();
        }
        return size;
    }
}
