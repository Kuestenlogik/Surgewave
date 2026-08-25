namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Where a plugin lives, which decides who owns it and what happens to it on an
/// upgrade. The distinction exists because "the plugins directory" used to mean
/// six different things depending on which process asked (#158).
/// </summary>
public enum PluginScope
{
    /// <summary>
    /// Inside the installation, next to the executable. Shipped with the artefact —
    /// the publish script writes here and container images bake here — so an
    /// upgrade replaces it wholesale. Nothing an operator installs belongs here:
    /// it would not survive the next deployment.
    /// </summary>
    Installation,

    /// <summary>
    /// Machine-wide state: what an operator installed on this host. Survives
    /// upgrades and is visible to the broker no matter which account it runs
    /// under, which is the whole point — a service does not run as the admin who
    /// installed the plugin, so the user profile is the wrong place for it.
    /// Writing here needs elevation.
    /// </summary>
    Machine,

    /// <summary>
    /// The current user's own plugins. Correct for a broker someone runs on their
    /// own machine under their own account, and invisible to a service running as
    /// another account — which is why it is an explicit choice rather than a
    /// fallback for when the machine scope turns out not to be writable.
    /// </summary>
    User
}
