using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// File-backed persistence for the alerting state. Writes are atomic
/// (temp file + move) so a crash mid-save never corrupts the store.
/// </summary>
/// <remarks>
/// Notification channel configs (Slack/Teams webhook URLs, PagerDuty routing
/// keys) are bearer-secret-equivalent and are stored here in cleartext. On
/// Unix the file is created owner-read/write only; on Windows it inherits the
/// content-root ACLs. Encryption-at-rest (ASP.NET Data Protection) is tracked
/// as a follow-up — keep the content root off world-readable locations.
/// </remarks>
public sealed class AlertingStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _path;
    private readonly ILogger<AlertingStore>? _logger;

    public AlertingStore(string path, ILogger<AlertingStore>? logger = null)
    {
        _path = path;
        _logger = logger;
    }

    /// <summary>Load the persisted state; returns an empty document when the file is missing or unreadable.</summary>
    public AlertingStateDocument Load()
    {
        try
        {
            if (!File.Exists(_path))
                return new AlertingStateDocument();

            var json = File.ReadAllText(_path);
            var document = JsonSerializer.Deserialize<AlertingStateDocument>(json, SerializerOptions)
                ?? new AlertingStateDocument();
            return Normalize(document);
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            _logger?.LogWarning(ex, "Could not read alerting state from {Path} — starting with empty state", _path);
            return new AlertingStateDocument();
        }
    }

    public void Save(AlertingStateDocument state)
    {
        try
        {
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            var tempPath = _path + ".tmp";
            File.WriteAllText(tempPath, JsonSerializer.Serialize(state, SerializerOptions));
            RestrictPermissions(tempPath);
            File.Move(tempPath, _path, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger?.LogError(ex, "Could not persist alerting state to {Path}", _path);
        }
    }

    /// <summary>
    /// Coalesce null collections (a hand-edited or foreign-tool-written file may
    /// contain <c>"rules": null</c>) to empty lists and drop null elements, so a
    /// structurally-valid-but-malformed file behaves like an empty store instead
    /// of throwing a NullReferenceException that would fault the evaluation
    /// worker and crash-loop the host.
    /// </summary>
    private static AlertingStateDocument Normalize(AlertingStateDocument document)
    {
        document.Rules = document.Rules?.Where(r => r is not null).ToList() ?? [];
        document.Channels = document.Channels?.Where(c => c is not null).ToList() ?? [];
        document.Events = document.Events?.Where(e => e is not null).ToList() ?? [];
        return document;
    }

    private void RestrictPermissions(string path)
    {
        if (OperatingSystem.IsWindows())
            return;

        try
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            _logger?.LogDebug(ex, "Could not restrict permissions on {Path}", path);
        }
    }
}
