namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Checks version compatibility between a local broker and the rest of the cluster.
/// Ensures that all brokers in the cluster can operate together during a rolling upgrade.
/// </summary>
public sealed class VersionCompatibilityChecker
{
    /// <summary>
    /// Check whether a local broker version is compatible with all cluster broker versions.
    /// </summary>
    /// <param name="local">The version of the local broker.</param>
    /// <param name="clusterVersions">The versions of all other brokers in the cluster.</param>
    /// <returns>A <see cref="CompatibilityResult"/> indicating compatibility status.</returns>
    public CompatibilityResult Check(BrokerVersion local, IReadOnlyList<BrokerVersion> clusterVersions)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(clusterVersions);

        if (clusterVersions.Count == 0)
        {
            return new CompatibilityResult(true, null, []);
        }

        var warnings = new List<string>();
        var incompatibleBrokers = new List<string>();

        foreach (var remote in clusterVersions)
        {
            if (!local.IsCompatibleWith(remote))
            {
                incompatibleBrokers.Add(
                    $"Broker version {remote} is incompatible with local version {local} (different major version)");
            }
            else if (local.Minor != remote.Minor)
            {
                warnings.Add(
                    $"Minor version mismatch: local={local}, remote={remote}. " +
                    "This is acceptable during a rolling upgrade but all brokers should reach the same version.");
            }
            else if (local.Patch != remote.Patch)
            {
                warnings.Add(
                    $"Patch version mismatch: local={local}, remote={remote}.");
            }
        }

        if (incompatibleBrokers.Count > 0)
        {
            var reason = $"Incompatible broker versions detected: {string.Join("; ", incompatibleBrokers)}";
            return new CompatibilityResult(false, reason, warnings);
        }

        // Check if more than 2 distinct versions are present (risky multi-hop upgrade)
        var distinctMajorMinor = clusterVersions
            .Append(local)
            .Select(v => $"{v.Major}.{v.Minor}")
            .Distinct()
            .Count();

        if (distinctMajorMinor > 2)
        {
            warnings.Add(
                "More than 2 distinct minor versions detected in the cluster. " +
                "Complete the current rolling upgrade before starting another.");
        }

        return new CompatibilityResult(true, null, warnings);
    }
}
