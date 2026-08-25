namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Where installed connectors live. Same three scopes as
/// <see cref="SurgewavePluginDirectories"/> and for the same reason (#158): a
/// connector is code the broker loads, so where it lives decides whether the
/// broker can see it whichever account it runs under.
/// </summary>
/// <remarks>
/// The two sides disagreed here as well: <c>surgewave plugins install
/// --from-nuget</c> put connectors in <c>~/.surgewave/connectors</c> while the
/// Connect plugin scanned <c>plugins</c> relative to the working directory. They
/// have never named the same directory, so the default path only worked for
/// whoever passed both flags.
/// </remarks>
public static class SurgewaveConnectorDirectories
{
    private const string DirectoryName = "connectors";

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
    /// The directories to search, in increasing order of precedence. See
    /// <see cref="SurgewavePluginDirectories.SearchOrder"/> — the rules are the
    /// same, including that an explicit directory is the only one searched.
    /// </summary>
    public static IReadOnlyList<string> SearchOrder(
        string? configuredDirectory = null,
        bool allowUserScope = true)
    {
        if (!string.IsNullOrWhiteSpace(configuredDirectory))
            return [Path.GetFullPath(configuredDirectory)];

        string[] scopes = allowUserScope
            ? [Installation, Machine, User]
            : [Installation, Machine];

        return scopes
            .Select(Path.GetFullPath)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }
}
