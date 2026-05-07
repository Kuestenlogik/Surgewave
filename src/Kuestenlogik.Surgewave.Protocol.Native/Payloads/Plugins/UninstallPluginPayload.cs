namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for UninstallPlugin request.
/// </summary>
public readonly record struct UninstallPluginRequestPayload
{
    public string PackageId { get; init; }
    public bool RemoveDependencies { get; init; }

    public static UninstallPluginRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new UninstallPluginRequestPayload
        {
            PackageId = reader.ReadString() ?? string.Empty,
            RemoveDependencies = reader.ReadBoolean()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteBoolean(RemoveDependencies);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteBoolean(RemoveDependencies);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "") + 1;
    }
}

/// <summary>
/// Wire format for UninstallPlugin response.
/// </summary>
public readonly record struct UninstallPluginResponsePayload
{
    public bool IsSuccess { get; init; }
    public IReadOnlyList<string> RemovedPackages { get; init; }
    public string? Error { get; init; }

    public static UninstallPluginResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var isSuccess = reader.ReadBoolean();

        var count = reader.ReadInt32();
        var removedPackages = new List<string>(count);
        for (int i = 0; i < count; i++)
        {
            removedPackages.Add(reader.ReadString() ?? string.Empty);
        }

        return new UninstallPluginResponsePayload
        {
            IsSuccess = isSuccess,
            RemovedPackages = removedPackages,
            Error = reader.ReadNullableString()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteBoolean(IsSuccess);

        writer.WriteInt32(RemovedPackages?.Count ?? 0);
        if (RemovedPackages != null)
        {
            foreach (var pkg in RemovedPackages)
            {
                writer.WriteString(pkg);
            }
        }

        writer.WriteNullableString(Error);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteBoolean(IsSuccess);

        writer.WriteInt32(RemovedPackages?.Count ?? 0);
        if (RemovedPackages != null)
        {
            foreach (var pkg in RemovedPackages)
            {
                writer.WriteString(pkg);
            }
        }

        writer.WriteNullableString(Error);
    }

    public int EstimateSize()
    {
        int size = 1 + 4; // bool + count

        if (RemovedPackages != null)
        {
            foreach (var pkg in RemovedPackages)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(pkg ?? "");
            }
        }

        size += 1 + (Error != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(Error) : 0);

        return size;
    }
}
