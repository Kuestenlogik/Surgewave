namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for InstallPlugin request.
/// </summary>
public readonly record struct InstallPluginRequestPayload
{
    public string PackageId { get; init; }
    public string? Version { get; init; }
    public bool IncludeDependencies { get; init; }

    public static InstallPluginRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new InstallPluginRequestPayload
        {
            PackageId = reader.ReadString() ?? string.Empty,
            Version = reader.ReadNullableString(),
            IncludeDependencies = reader.ReadBoolean()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteNullableString(Version);
        writer.WriteBoolean(IncludeDependencies);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteNullableString(Version);
        writer.WriteBoolean(IncludeDependencies);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "")
             + 1 + (Version != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(Version) : 0)
             + 1;
    }
}

/// <summary>
/// Information about an installed plugin package.
/// </summary>
public readonly record struct InstalledPackageInfoPayload
{
    public string PackageId { get; init; }
    public string Version { get; init; }
    public bool WasDependency { get; init; }

    public static InstalledPackageInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        return new InstalledPackageInfoPayload
        {
            PackageId = reader.ReadString() ?? string.Empty,
            Version = reader.ReadString() ?? string.Empty,
            WasDependency = reader.ReadBoolean()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Version);
        writer.WriteBoolean(WasDependency);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Version);
        writer.WriteBoolean(WasDependency);
    }

    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "")
             + 2 + System.Text.Encoding.UTF8.GetByteCount(Version ?? "")
             + 1;
    }
}

/// <summary>
/// Wire format for InstallPlugin response.
/// </summary>
public readonly record struct InstallPluginResponsePayload
{
    public bool IsSuccess { get; init; }
    public bool IsPartialSuccess { get; init; }
    public IReadOnlyList<InstalledPackageInfoPayload> InstalledPackages { get; init; }
    public IReadOnlyList<string> Errors { get; init; }

    public static InstallPluginResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var isSuccess = reader.ReadBoolean();
        var isPartialSuccess = reader.ReadBoolean();

        // Read installed packages
        var pkgCount = reader.ReadInt32();
        var installedPackages = new List<InstalledPackageInfoPayload>(pkgCount);
        for (int i = 0; i < pkgCount; i++)
        {
            installedPackages.Add(InstalledPackageInfoPayload.Read(ref reader));
        }

        // Read errors
        var errCount = reader.ReadInt32();
        var errors = new List<string>(errCount);
        for (int i = 0; i < errCount; i++)
        {
            errors.Add(reader.ReadString() ?? string.Empty);
        }

        return new InstallPluginResponsePayload
        {
            IsSuccess = isSuccess,
            IsPartialSuccess = isPartialSuccess,
            InstalledPackages = installedPackages,
            Errors = errors
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteBoolean(IsSuccess);
        writer.WriteBoolean(IsPartialSuccess);

        // Write installed packages
        writer.WriteInt32(InstalledPackages?.Count ?? 0);
        if (InstalledPackages != null)
        {
            foreach (var pkg in InstalledPackages)
            {
                pkg.Write(ref writer);
            }
        }

        // Write errors
        writer.WriteInt32(Errors?.Count ?? 0);
        if (Errors != null)
        {
            foreach (var err in Errors)
            {
                writer.WriteString(err);
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteBoolean(IsSuccess);
        writer.WriteBoolean(IsPartialSuccess);

        // Write installed packages
        writer.WriteInt32(InstalledPackages?.Count ?? 0);
        if (InstalledPackages != null)
        {
            foreach (var pkg in InstalledPackages)
            {
                pkg.WriteTo(writer);
            }
        }

        // Write errors
        writer.WriteInt32(Errors?.Count ?? 0);
        if (Errors != null)
        {
            foreach (var err in Errors)
            {
                writer.WriteString(err);
            }
        }
    }

    public int EstimateSize()
    {
        int size = 1 + 1 + 4 + 4; // bools + counts

        if (InstalledPackages != null)
        {
            foreach (var pkg in InstalledPackages)
            {
                size += pkg.EstimateSize();
            }
        }

        if (Errors != null)
        {
            foreach (var err in Errors)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(err ?? "");
            }
        }

        return size;
    }
}
