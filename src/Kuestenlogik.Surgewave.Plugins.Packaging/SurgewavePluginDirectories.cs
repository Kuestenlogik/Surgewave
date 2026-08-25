namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Resolves where installed plugins live, for every process that needs to know
/// (#158). Before this existed the answer was computed independently in the
/// broker, the CLI, Control, the Connect worker and the marketplace, and the
/// first two disagreed: the CLI installed into <c>plugins</c> relative to the
/// working directory while the broker read <c>plugins</c> next to its own
/// executable, so installing a plugin reported success and changed nothing
/// (#157).
/// </summary>
public static class SurgewavePluginDirectories
{
    private const string DirectoryName = "plugins";

    /// <summary>Shipped inside the installation, next to the executable.</summary>
    public static string Installation => Path.Combine(AppContext.BaseDirectory, DirectoryName);

    /// <summary>Machine-wide, what an operator installed on this host.</summary>
    public static string Machine => Path.Combine(SurgewaveDataRoot.Machine, DirectoryName);

    /// <summary>The current user's own.</summary>
    public static string User => Path.Combine(SurgewaveDataRoot.User, DirectoryName);

    /// <summary>The directory for one scope.</summary>
    public static string Resolve(PluginScope scope) => scope switch
    {
        PluginScope.Installation => Installation,
        PluginScope.Machine => Machine,
        PluginScope.User => User,
        _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "Unknown plugin scope")
    };

    /// <summary>
    /// The directories to search, in increasing order of precedence: a plugin id
    /// found later replaces the same id found earlier.
    /// </summary>
    /// <param name="configuredDirectory">
    /// An explicit <c>Surgewave:PluginsDirectory</c>. When set it is the only
    /// directory searched — a container image that mounts one directory wants
    /// exactly that and nothing else, and silently adding host directories to it
    /// would make the image's behaviour depend on the host.
    /// </param>
    /// <remarks>
    /// Installation first, then machine, then user: the further a directory is
    /// from the artefact, the more deliberate the act of putting something there
    /// was. An operator installing a newer build of a bundled plugin expects
    /// theirs to win, and a developer's own copy wins over both — but only for a
    /// broker running under that developer's account, which is precisely what
    /// makes the user scope safe to include here.
    /// </remarks>
    public static IReadOnlyList<string> SearchOrder(string? configuredDirectory = null)
    {
        if (!string.IsNullOrWhiteSpace(configuredDirectory))
            return [Path.GetFullPath(configuredDirectory)];

        // Distinct: the SURGEWAVE_DATA_DIR override collapses machine and user onto
        // one root, and scanning the same directory twice would log every plugin in
        // it as shadowing itself.
        return new[] { Installation, Machine, User }
            .Select(Path.GetFullPath)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }
}
