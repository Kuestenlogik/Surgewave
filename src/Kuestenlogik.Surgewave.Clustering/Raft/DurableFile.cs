using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// The file operations whose result has to be there after a power loss, not only after the process ends.
/// </summary>
/// <remarks>
/// Writing a file and flushing it makes its bytes durable. That the file exists under its name, or that a rename took
/// place, is a change to the directory, and durable only once the directory is: a state file replaced by a rename
/// could be the one from before again after a power loss, with the term and the vote of before.
/// </remarks>
internal static class DurableFile
{
    private const uint MoveFileReplaceExisting = 0x1;
    private const uint MoveFileWriteThrough = 0x8;

    /// <summary>
    /// Flushes what was written to <paramref name="file"/> to the disk.
    /// </summary>
    public static void FlushToDisk(FileStream file) => file.Flush(flushToDisk: true);

    /// <summary>
    /// Puts <paramref name="source"/> in the place of <paramref name="target"/> and returns once that is on disk.
    /// </summary>
    public static void Replace(string source, string target)
    {
        if (OperatingSystem.IsWindows())
        {
            // Returns only once the move is flushed to disk, which File.Move does not ask for.
            if (!MoveFileExW(source, target, MoveFileReplaceExisting | MoveFileWriteThrough))
            {
                throw new IOException(
                    $"Could not replace '{target}' with '{source}': {new Win32Exception(Marshal.GetLastPInvokeError()).Message}");
            }

            return;
        }

        File.Move(source, target, overwrite: true);
        SyncDirectory(Path.GetDirectoryName(target)!);
    }

    /// <summary>
    /// Makes the entries of <paramref name="directory"/> durable: a file created in it, or renamed.
    /// </summary>
    /// <remarks>
    /// Nothing to do on Windows, which has no flush for a directory a process could ask for: a file's own flush
    /// covers its creation there, and <see cref="Replace"/> asks for the rename to be written through.
    /// </remarks>
    public static void SyncDirectory(string directory)
    {
        if (OperatingSystem.IsWindows())
            return;

        SyncUnixDirectory(directory);
    }

    [UnsupportedOSPlatform("windows")]
    private static void SyncUnixDirectory(string directory)
    {
        // O_RDONLY, which is 0 on Linux and on macOS; .NET's own file handles refuse a directory. The path as the
        // bytes libc takes, with their end.
        var descriptor = open(Encoding.UTF8.GetBytes(directory + '\0'), 0);
        if (descriptor < 0)
            throw new IOException($"Could not open the directory '{directory}' to flush it: errno {Marshal.GetLastPInvokeError()}");

        try
        {
            if (fsync(descriptor) != 0)
                throw new IOException($"Could not flush the directory '{directory}': errno {Marshal.GetLastPInvokeError()}");
        }
        finally
        {
            _ = close(descriptor);
        }
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [SupportedOSPlatform("windows")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool MoveFileExW(string existingFileName, string newFileName, uint flags);

    [DllImport("libc", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [UnsupportedOSPlatform("windows")]
    private static extern int open(byte[] path, int flags);

    [DllImport("libc", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [UnsupportedOSPlatform("windows")]
    private static extern int fsync(int descriptor);

    [DllImport("libc", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [UnsupportedOSPlatform("windows")]
    private static extern int close(int descriptor);
}
