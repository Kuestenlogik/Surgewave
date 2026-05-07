using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

/// <summary>
/// Wire format for plugin package information.
/// </summary>
[SuppressMessage("Design", "CA1056:URI-like parameters should not be strings",
    Justification = "Wire protocol uses strings for URLs to avoid Uri serialization overhead")]
public readonly record struct PluginInfoPayload
{
    public string PackageId { get; init; }
    public string Name { get; init; }
    public string Version { get; init; }
    public string? Description { get; init; }
    public string? Author { get; init; }
    public string? License { get; init; }
    public string? ProjectUrl { get; init; }
    public string? IconUrl { get; init; }
    public bool IsInstalled { get; init; }
    public string? InstalledVersion { get; init; }
    public long DownloadCount { get; init; }
    public IReadOnlyList<string> ConnectorTypes { get; init; }
    public IReadOnlyList<string> Tags { get; init; }
    public IReadOnlyList<string> AvailableVersions { get; init; }
    public IReadOnlyList<PluginDependencyPayload> Dependencies { get; init; }
    public bool IsSigned { get; init; }
    public string? SignerIdentity { get; init; }
    public string? SignerProvider { get; init; }

    public static PluginInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        var packageId = reader.ReadString() ?? string.Empty;
        var name = reader.ReadString() ?? string.Empty;
        var version = reader.ReadString() ?? string.Empty;
        var description = reader.ReadNullableString();
        var author = reader.ReadNullableString();
        var license = reader.ReadNullableString();
        var projectUrl = reader.ReadNullableString();
        var iconUrl = reader.ReadNullableString();
        var isInstalled = reader.ReadBoolean();
        var installedVersion = reader.ReadNullableString();
        var downloadCount = reader.ReadInt64();

        // Read connector types
        var connectorTypeCount = reader.ReadInt32();
        var connectorTypes = new List<string>(connectorTypeCount);
        for (int i = 0; i < connectorTypeCount; i++)
        {
            connectorTypes.Add(reader.ReadString() ?? string.Empty);
        }

        // Read tags
        var tagCount = reader.ReadInt32();
        var tags = new List<string>(tagCount);
        for (int i = 0; i < tagCount; i++)
        {
            tags.Add(reader.ReadString() ?? string.Empty);
        }

        // Read available versions
        var versionCount = reader.ReadInt32();
        var availableVersions = new List<string>(versionCount);
        for (int i = 0; i < versionCount; i++)
        {
            availableVersions.Add(reader.ReadString() ?? string.Empty);
        }

        // Read dependencies
        var depCount = reader.ReadInt32();
        var dependencies = new List<PluginDependencyPayload>(depCount);
        for (int i = 0; i < depCount; i++)
        {
            dependencies.Add(PluginDependencyPayload.Read(ref reader));
        }

        // Signature metadata (appended in v0.2 of the payload).
        var isSigned = reader.ReadBoolean();
        var signerIdentity = reader.ReadNullableString();
        var signerProvider = reader.ReadNullableString();

        return new PluginInfoPayload
        {
            PackageId = packageId,
            Name = name,
            Version = version,
            Description = description,
            Author = author,
            License = license,
            ProjectUrl = projectUrl,
            IconUrl = iconUrl,
            IsInstalled = isInstalled,
            InstalledVersion = installedVersion,
            DownloadCount = downloadCount,
            ConnectorTypes = connectorTypes,
            Tags = tags,
            AvailableVersions = availableVersions,
            Dependencies = dependencies,
            IsSigned = isSigned,
            SignerIdentity = signerIdentity,
            SignerProvider = signerProvider
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Name);
        writer.WriteString(Version);
        writer.WriteNullableString(Description);
        writer.WriteNullableString(Author);
        writer.WriteNullableString(License);
        writer.WriteNullableString(ProjectUrl);
        writer.WriteNullableString(IconUrl);
        writer.WriteBoolean(IsInstalled);
        writer.WriteNullableString(InstalledVersion);
        writer.WriteInt64(DownloadCount);

        // Write connector types
        writer.WriteInt32(ConnectorTypes?.Count ?? 0);
        if (ConnectorTypes != null)
        {
            foreach (var type in ConnectorTypes)
            {
                writer.WriteString(type);
            }
        }

        // Write tags
        writer.WriteInt32(Tags?.Count ?? 0);
        if (Tags != null)
        {
            foreach (var tag in Tags)
            {
                writer.WriteString(tag);
            }
        }

        // Write available versions
        writer.WriteInt32(AvailableVersions?.Count ?? 0);
        if (AvailableVersions != null)
        {
            foreach (var v in AvailableVersions)
            {
                writer.WriteString(v);
            }
        }

        // Write dependencies
        writer.WriteInt32(Dependencies?.Count ?? 0);
        if (Dependencies != null)
        {
            foreach (var dep in Dependencies)
            {
                dep.Write(ref writer);
            }
        }

        // Signature metadata (v0.2)
        writer.WriteBoolean(IsSigned);
        writer.WriteNullableString(SignerIdentity);
        writer.WriteNullableString(SignerProvider);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(PackageId);
        writer.WriteString(Name);
        writer.WriteString(Version);
        writer.WriteNullableString(Description);
        writer.WriteNullableString(Author);
        writer.WriteNullableString(License);
        writer.WriteNullableString(ProjectUrl);
        writer.WriteNullableString(IconUrl);
        writer.WriteBoolean(IsInstalled);
        writer.WriteNullableString(InstalledVersion);
        writer.WriteInt64(DownloadCount);

        // Write connector types
        writer.WriteInt32(ConnectorTypes?.Count ?? 0);
        if (ConnectorTypes != null)
        {
            foreach (var type in ConnectorTypes)
            {
                writer.WriteString(type);
            }
        }

        // Write tags
        writer.WriteInt32(Tags?.Count ?? 0);
        if (Tags != null)
        {
            foreach (var tag in Tags)
            {
                writer.WriteString(tag);
            }
        }

        // Write available versions
        writer.WriteInt32(AvailableVersions?.Count ?? 0);
        if (AvailableVersions != null)
        {
            foreach (var v in AvailableVersions)
            {
                writer.WriteString(v);
            }
        }

        // Write dependencies
        writer.WriteInt32(Dependencies?.Count ?? 0);
        if (Dependencies != null)
        {
            foreach (var dep in Dependencies)
            {
                dep.WriteTo(writer);
            }
        }

        // Signature metadata (v0.2)
        writer.WriteBoolean(IsSigned);
        writer.WriteNullableString(SignerIdentity);
        writer.WriteNullableString(SignerProvider);
    }

    public int EstimateSize()
    {
        int size = 0;

        size += 2 + System.Text.Encoding.UTF8.GetByteCount(PackageId ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(Version ?? "");
        size += 1 + (Description != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(Description) : 0);
        size += 1 + (Author != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(Author) : 0);
        size += 1 + (License != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(License) : 0);
        size += 1 + (ProjectUrl != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(ProjectUrl) : 0);
        size += 1 + (IconUrl != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(IconUrl) : 0);
        size += 1; // IsInstalled
        size += 1 + (InstalledVersion != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(InstalledVersion) : 0);
        size += 8; // DownloadCount (int64)

        // Connector types
        size += 4;
        if (ConnectorTypes != null)
        {
            foreach (var type in ConnectorTypes)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(type ?? "");
            }
        }

        // Tags
        size += 4;
        if (Tags != null)
        {
            foreach (var tag in Tags)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(tag ?? "");
            }
        }

        // Available versions
        size += 4;
        if (AvailableVersions != null)
        {
            foreach (var v in AvailableVersions)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(v ?? "");
            }
        }

        // Dependencies
        size += 4;
        if (Dependencies != null)
        {
            foreach (var dep in Dependencies)
            {
                size += dep.EstimateSize();
            }
        }

        // Signature metadata
        size += 1; // IsSigned
        size += 1 + (SignerIdentity != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(SignerIdentity) : 0);
        size += 1 + (SignerProvider != null ? 2 + System.Text.Encoding.UTF8.GetByteCount(SignerProvider) : 0);

        return size;
    }
}
