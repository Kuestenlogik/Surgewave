using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Locks down the dependency-tree traversal helpers (<see cref="DependencyTreeNode.Flatten"/>,
/// <see cref="DependencyTreeNode.GetMissingDependencies"/>, <see cref="DependencyTreeNode.GetUninstalledDependencies"/>,
/// <see cref="DependencyTreeNode.TotalDependencyCount"/>, <see cref="DependencyTreeNode.MaxDepth"/>).
/// These feed both the CLI-Tree-Print and the resolver's "what do I need to install"-pass.
/// </summary>
public sealed class DependencyTreeNodeTests
{
    private static DependencyTreeNode Leaf(string id, string version, bool installed = true) => new()
    {
        PackageId = id,
        Version = version,
        IsInstalled = installed,
        InstalledVersion = installed ? version : null,
    };

    [Fact]
    public void Flatten_SingleNode_YieldsItself()
    {
        var node = Leaf("a", "1.0.0");

        var flat = node.Flatten().ToList();

        Assert.Single(flat);
        Assert.Same(node, flat[0]);
    }

    [Fact]
    public void Flatten_NestedTree_YieldsParentBeforeChildren()
    {
        var leaf1 = Leaf("c", "1.0.0");
        var leaf2 = Leaf("d", "1.0.0");
        var branch = Leaf("b", "1.0.0") with { Children = [leaf1, leaf2] };
        var root = Leaf("a", "1.0.0") with { Children = [branch] };

        var flat = root.Flatten().ToList();

        Assert.Equal(4, flat.Count);
        Assert.Equal("a", flat[0].PackageId);
        Assert.Equal("b", flat[1].PackageId);
        Assert.Equal("c", flat[2].PackageId);
        Assert.Equal("d", flat[3].PackageId);
    }

    [Fact]
    public void TotalDependencyCount_DeepTree()
    {
        var tree = Leaf("root", "1.0.0") with
        {
            Children =
            [
                Leaf("a", "1.0.0") with { Children = [Leaf("a1", "1.0.0"), Leaf("a2", "1.0.0")] },
                Leaf("b", "1.0.0"),
            ],
        };

        // a, a1, a2, b — 4 children total
        Assert.Equal(4, tree.TotalDependencyCount);
    }

    [Fact]
    public void TotalDependencyCount_LeafNode_IsZero()
    {
        Assert.Equal(0, Leaf("x", "1.0.0").TotalDependencyCount);
    }

    [Fact]
    public void MaxDepth_LeafNode_IsZero()
    {
        Assert.Equal(0, Leaf("x", "1.0.0").MaxDepth);
    }

    [Fact]
    public void MaxDepth_NestedTree_ReturnsLongestPath()
    {
        var tree = Leaf("root", "1.0.0") with
        {
            Children =
            [
                Leaf("a", "1.0.0"),
                Leaf("b", "1.0.0") with
                {
                    Children = [Leaf("b1", "1.0.0") with { Children = [Leaf("b1a", "1.0.0")] }],
                },
            ],
        };

        // root -> b -> b1 -> b1a = depth 3
        Assert.Equal(3, tree.MaxDepth);
    }

    [Fact]
    public void GetMissingDependencies_FindsOnlyMissingNodes()
    {
        var tree = Leaf("root", "1.0.0") with
        {
            Children =
            [
                Leaf("installed", "1.0.0", installed: true),
                Leaf("gone", "1.0.0", installed: false) with { IsMissing = true },
                Leaf("nested", "1.0.0", installed: true) with
                {
                    Children = [Leaf("deep-missing", "1.0.0", installed: false) with { IsMissing = true }],
                },
            ],
        };

        var missing = tree.GetMissingDependencies().Select(n => n.PackageId).ToList();

        Assert.Equal(2, missing.Count);
        Assert.Contains("gone", missing);
        Assert.Contains("deep-missing", missing);
    }

    [Fact]
    public void GetUninstalledDependencies_ExcludesMissingAndCircular()
    {
        var tree = Leaf("root", "1.0.0") with
        {
            Children =
            [
                Leaf("uninstalled", "1.0.0", installed: false),
                Leaf("missing", "1.0.0", installed: false) with { IsMissing = true },
                Leaf("circular", "1.0.0", installed: false) with { IsCircular = true },
                Leaf("installed", "1.0.0"),
            ],
        };

        var uninstalled = tree.GetUninstalledDependencies().Select(n => n.PackageId).ToList();

        Assert.Single(uninstalled, "uninstalled");
    }

    [Fact]
    public void ToTreeString_IncludesPackageIdVersionAndStatusBracket()
    {
        var tree = Leaf("root", "1.0.0") with
        {
            Children = [Leaf("dep", "2.0.0")],
        };

        var s = tree.ToTreeString();

        Assert.Contains("root@1.0.0", s);
        Assert.Contains("dep@2.0.0", s);
        Assert.Contains("[installed]", s);
    }

    [Fact]
    public void ToTreeString_MissingNode_LabeledMissing()
    {
        var node = Leaf("gone", "1.0.0", installed: false) with { IsMissing = true };
        Assert.Contains("[missing]", node.ToTreeString());
    }

    [Fact]
    public void ToTreeString_MissingOptional_LabeledMissingOptional()
    {
        var node = Leaf("gone", "1.0.0", installed: false) with { IsMissing = true, IsOptional = true };
        Assert.Contains("[missing, optional]", node.ToTreeString());
    }

    [Fact]
    public void ToTreeString_Circular_LabeledCircular()
    {
        var node = Leaf("loop", "1.0.0") with { IsCircular = true };
        Assert.Contains("[circular]", node.ToTreeString());
    }

    [Fact]
    public void ToTreeString_VersionMismatch_ShowsInstalledVersion()
    {
        var node = new DependencyTreeNode
        {
            PackageId = "a",
            Version = "2.0.0",
            InstalledVersion = "1.5.0",
            IsInstalled = true,
        };

        Assert.Contains("[installed: 1.5.0]", node.ToTreeString());
    }
}
