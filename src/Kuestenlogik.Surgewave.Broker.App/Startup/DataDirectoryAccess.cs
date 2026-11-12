namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Whether the broker can write to its data directory, asked once before anything opens it.
/// </summary>
/// <remarks>
/// A directory the process may not write to used to end the start with the stack trace of the first component that
/// wrote, <c>Access to the path '/app/data/meta.properties.tmp' is denied</c>, and exit code 139; where the directory
/// itself was writable and what an earlier start had left in it was not, the broker logged a failed transition for
/// every partition and never became ready. The usual cause is a volume an earlier deployment wrote as another user:
/// the container image runs as an unprivileged user, and a deployment that ran it as root left a volume only root
/// can write to.
/// </remarks>
public static class DataDirectoryAccess
{
    /// <summary>
    /// Creates <paramref name="dataDirectory"/> when it is missing and tries what the broker will do in it: create a
    /// file in the directory and in each directory directly below it, and open the files directly in it for writing.
    /// Returns <see langword="null"/> when all of that works, otherwise what is wrong and how to mend it. Nothing is
    /// left behind and no file is changed.
    /// </summary>
    public static string? Problem(string dataDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDirectory);

        var directory = Path.GetFullPath(dataDirectory);
        try
        {
            Directory.CreateDirectory(directory);
            CreateAndRemoveAFile(directory);
            foreach (var below in Directory.EnumerateDirectories(directory))
                CreateAndRemoveAFile(below);
            foreach (var file in Directory.EnumerateFiles(directory))
                File.Open(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite).Dispose();
            return null;
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            var user = Environment.UserName;
            var mend = OperatingSystem.IsWindows()
                ? $"Give '{user}' write access to it and to everything in it, or set Surgewave:DataDirectory to a "
                    + "directory it may write to."
                : $"If an earlier deployment wrote it as another user - a container that ran the broker as root - give it "
                    + $"to this user once, as root: chown -R {user} {directory}. Otherwise set Surgewave:DataDirectory "
                    + "to a directory this user may write to.";
            return $"The data directory '{directory}' is not writable for user '{user}': {ex.Message} {mend}";
        }
    }

    private static void CreateAndRemoveAFile(string directory)
    {
        var probe = Path.Combine(directory, $".write-probe-{Guid.NewGuid():N}");
        File.WriteAllBytes(probe, []);
        File.Delete(probe);
    }
}
