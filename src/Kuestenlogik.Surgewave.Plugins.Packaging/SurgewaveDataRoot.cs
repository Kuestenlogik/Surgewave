namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// The directories Surgewave keeps data under, separated by who owns it (#158).
/// </summary>
/// <remarks>
/// <para>
/// Data used to land relative to whatever the current working directory happened
/// to be, which meant it landed in a different place for every way of starting
/// the same program: <c>bin/Debug/</c> under the debugger, <c>bin/</c> from a
/// published run, the repository root from <c>dotnet run</c>, a test's own
/// output directory under the runner. Nothing could be reliably found, and
/// nothing could be reliably cleaned up either — a test suite had to guess where
/// its predecessor had written.
/// </para>
/// <para>
/// <see cref="Override"/> is the answer to the second half of that: point
/// <c>SURGEWAVE_DATA_DIR</c> at a temporary directory and every scope moves
/// underneath it, so a test fixture creates one tree and deletes one tree. It
/// deliberately overrides <em>both</em> scopes rather than just one — a test that
/// still wrote machine-wide state while its user state was redirected would be
/// the worst of both.
/// </para>
/// <para>
/// <see cref="Instance"/> covers the other axis: several brokers on one machine.
/// It is the pattern PostgreSQL uses with <c>PGDATA</c> and GeoServer with
/// <c>GEOSERVER_DATA_DIR</c> — the service binary is interchangeable, the data
/// directory is what identifies a running thing. Unset means the scope is the
/// root itself, so a single-instance host keeps the short paths and naming an
/// instance later relocates only that one.
/// </para>
/// <para>
/// The platform branch is written out by hand rather than taken from
/// <see cref="Environment.SpecialFolder.CommonApplicationData"/>, which .NET maps
/// to <c>/usr/share</c> on Unix. That is a location for static package data, not
/// for state a broker writes to; the FHS answer is <c>/var/lib</c>.
/// <c>RepositoryConfiguration</c> already branches the same way for its config
/// search paths.
/// </para>
/// </remarks>
public static class SurgewaveDataRoot
{
    /// <summary>
    /// Environment variable that relocates every scope under one directory.
    /// Set it in tests and in throwaway environments; leave it unset in
    /// production, where the point of the separation is that the scopes differ.
    /// </summary>
    public const string OverrideVariable = "SURGEWAVE_DATA_DIR";

    /// <summary>
    /// Environment variable naming this instance, so several brokers can run on one
    /// machine without sharing state.
    /// </summary>
    public const string InstanceVariable = "SURGEWAVE_INSTANCE";

    /// <summary>
    /// Subdirectory names this root owns. An instance may not be called any of
    /// these: with no instance set the scopes ARE the root, so an instance named
    /// "plugins" would resolve to the same directory the unnamed instance keeps its
    /// plugins in.
    /// </summary>
    private static readonly string[] ReservedNames = ["plugins", "connectors", "templates", "certs"];

    private static string? _configuredRoot;
    private static string? _configuredInstance;

    /// <summary>
    /// Sets the root and instance explicitly, overriding the environment.
    /// </summary>
    /// <remarks>
    /// The host calls this once at startup with the values its configuration
    /// resolved, which is what makes <c>--Surgewave:Instance=beta</c> on the command
    /// line and <c>Surgewave:Instance</c> in appsettings.json work as well as
    /// <c>SURGEWAVE_INSTANCE</c> — the configuration pipeline already layers all
    /// three, so this class does not have to. The environment variables stay the
    /// fallback for anything that starts before configuration exists, and for
    /// containers, where an environment variable is the natural handle.
    /// </remarks>
    /// <param name="dataRoot">Root for both scopes, or <c>null</c> to keep the environment's.</param>
    /// <param name="instance">Instance name, or <c>null</c> to keep the environment's.</param>
    public static void Configure(string? dataRoot = null, string? instance = null)
    {
        _configuredRoot = string.IsNullOrWhiteSpace(dataRoot) ? null : dataRoot.Trim();
        _configuredInstance = string.IsNullOrWhiteSpace(instance) ? null : instance.Trim();

        // Validate now rather than on first path resolution: a bad instance name
        // should stop startup with a clear message, not surface later as a
        // directory nobody expected.
        _ = Instance;
    }

    /// <summary>The override, or <c>null</c> when unset or empty.</summary>
    public static string? Override
    {
        get
        {
            if (_configuredRoot is not null) return _configuredRoot;
            var value = Environment.GetEnvironmentVariable(OverrideVariable);
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }
    }

    /// <summary>
    /// This instance's name, or <c>null</c> when unset. Note that the cluster id is
    /// deliberately not used for this: two nodes of the same cluster on one machine
    /// share it, and they are exactly the case that must not share a data directory.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The name contains a path separator, is a relative traversal, or collides with
    /// a directory this root already owns.
    /// </exception>
    public static string? Instance
    {
        get
        {
            var value = _configuredInstance
                ?? Environment.GetEnvironmentVariable(InstanceVariable);
            if (string.IsNullOrWhiteSpace(value)) return null;

            value = value.Trim();

            if (value.IndexOfAny([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar]) >= 0
                || value == "." || value == "..")
            {
                throw new InvalidOperationException(
                    $"{InstanceVariable} must be a single directory name, not a path: '{value}'. "
                    + $"To place the data somewhere else entirely, set {OverrideVariable}.");
            }

            if (ReservedNames.Contains(value, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"{InstanceVariable} may not be '{value}': that is a directory the data root "
                    + "already uses, so an instance of that name would share state with an "
                    + "unnamed one. Reserved names: " + string.Join(", ", ReservedNames) + ".");
            }

            return value;
        }
    }

    /// <summary>
    /// Appends the instance segment when one is set. Unset means the scope is the
    /// root itself — the single-instance case stays the simple one, and adding an
    /// instance name later moves that instance's data without disturbing anything
    /// that was already there.
    /// </summary>
    private static string WithInstance(string root)
    {
        var instance = Instance;
        return instance is null ? root : Path.Combine(root, instance);
    }

    /// <summary>
    /// Machine-wide state: <c>%ProgramData%\Surgewave</c> on Windows,
    /// <c>/var/lib/surgewave</c> elsewhere. Writing here needs elevation, which is
    /// intended — this is state that outlives the account that created it.
    /// </summary>
    public static string Machine => WithInstance(
        Override
        ?? (OperatingSystem.IsWindows()
            ? Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "Surgewave")
            : Path.Combine("/var", "lib", "surgewave")));

    /// <summary>
    /// The current user's own state, <c>~/.surgewave</c>. Already where plugin
    /// sources, pipeline templates and repository config live, so the name is not
    /// new — what is new is that it is no longer also the answer for things a
    /// service has to read.
    /// </summary>
    public static string User => WithInstance(
        Override
        ?? Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".surgewave"));
}
