namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Represents a node in the dependency tree.
/// </summary>
public sealed record DependencyTreeNode
{
    /// <summary>
    /// Package ID.
    /// </summary>
    public required string PackageId { get; init; }

    /// <summary>
    /// Package version (or version constraint).
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Currently installed version (null if not installed).
    /// </summary>
    public string? InstalledVersion { get; init; }

    /// <summary>
    /// Whether the package is installed.
    /// </summary>
    public bool IsInstalled { get; init; }

    /// <summary>
    /// Whether this is an optional dependency.
    /// </summary>
    public bool IsOptional { get; init; }

    /// <summary>
    /// Whether this node represents a circular dependency.
    /// </summary>
    public bool IsCircular { get; init; }

    /// <summary>
    /// Whether this dependency is missing (not found in any repository).
    /// </summary>
    public bool IsMissing { get; init; }

    /// <summary>
    /// Child dependencies.
    /// </summary>
    public IReadOnlyList<DependencyTreeNode> Children { get; init; } = [];

    /// <summary>
    /// Total number of dependencies (recursive).
    /// </summary>
    public int TotalDependencyCount => Children.Count + Children.Sum(c => c.TotalDependencyCount);

    /// <summary>
    /// Maximum depth of the dependency tree.
    /// </summary>
    public int MaxDepth => Children.Count == 0 ? 0 : 1 + Children.Max(c => c.MaxDepth);

    /// <summary>
    /// Flatten the tree to a list (breadth-first).
    /// </summary>
    public IEnumerable<DependencyTreeNode> Flatten()
    {
        yield return this;
        foreach (var child in Children)
        {
            foreach (var node in child.Flatten())
            {
                yield return node;
            }
        }
    }

    /// <summary>
    /// Get all missing dependencies.
    /// </summary>
    public IEnumerable<DependencyTreeNode> GetMissingDependencies()
    {
        return Flatten().Where(n => n.IsMissing);
    }

    /// <summary>
    /// Get all uninstalled dependencies.
    /// </summary>
    public IEnumerable<DependencyTreeNode> GetUninstalledDependencies()
    {
        return Flatten().Where(n => !n.IsInstalled && !n.IsMissing && !n.IsCircular);
    }

    /// <summary>
    /// Format as a tree string for display.
    /// </summary>
    public string ToTreeString(int indent = 0)
    {
        var prefix = new string(' ', indent * 2);
        var marker = indent == 0 ? "" : "├─ ";
        var status = GetStatusString();

        var result = $"{prefix}{marker}{PackageId}@{Version}{status}\n";

        for (var i = 0; i < Children.Count; i++)
        {
            result += Children[i].ToTreeString(indent + 1);
        }

        return result;
    }

    private string GetStatusString()
    {
        if (IsCircular) return " [circular]";
        if (IsMissing) return IsOptional ? " [missing, optional]" : " [missing]";
        if (!IsInstalled) return " [not installed]";
        if (InstalledVersion != Version) return $" [installed: {InstalledVersion}]";
        return " [installed]";
    }
}
