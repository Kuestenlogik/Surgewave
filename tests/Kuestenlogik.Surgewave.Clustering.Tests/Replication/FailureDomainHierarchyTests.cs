using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// Tests for FailureDomain and FailureDomainHierarchy.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class FailureDomainHierarchyTests
{
    [Fact]
    public void RegisterBroker_SimpleRack_CreatesSingleDomain()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act
        hierarchy.RegisterBroker(1, "rack-1");

        // Assert
        var domain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(domain);
        Assert.Equal("rack-1", domain.Name);
        Assert.Equal("rack-1", domain.Path);
        Assert.Equal(FailureDomainLevel.Rack, domain.Level);
        Assert.Contains(1, domain.Brokers);
    }

    [Fact]
    public void RegisterBroker_HierarchicalRack_CreatesMultipleLevels()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act - 4-level hierarchy: region/datacenter/zone/rack
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");

        // Assert - Rack is the leaf level (where broker is registered)
        var rackDomain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(rackDomain);
        Assert.Equal("rack-1", rackDomain.Name);
        Assert.Equal(FailureDomainLevel.Rack, rackDomain.Level);
        Assert.Equal("us-east/dc1/zone-a/rack-1", rackDomain.Path);

        // Verify parent domains exist by checking paths
        Assert.NotNull(rackDomain.Parent);
        Assert.Equal("zone-a", rackDomain.Parent.Name);
        Assert.Equal(FailureDomainLevel.Zone, rackDomain.Parent.Level);

        Assert.NotNull(rackDomain.Parent.Parent);
        Assert.Equal("dc1", rackDomain.Parent.Parent.Name);
        Assert.Equal(FailureDomainLevel.Datacenter, rackDomain.Parent.Parent.Level);

        Assert.NotNull(rackDomain.Parent.Parent.Parent);
        Assert.Equal("us-east", rackDomain.Parent.Parent.Parent.Name);
        Assert.Equal(FailureDomainLevel.Region, rackDomain.Parent.Parent.Parent.Level);
    }

    [Fact]
    public void RegisterBroker_NullRack_UsesDefaultDomain()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act
        hierarchy.RegisterBroker(1, null);

        // Assert
        var domain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(domain);
        Assert.Equal("default", domain.Name);
    }

    [Fact]
    public void RegisterBroker_MultipleBrokersSameRack_SharesDomain()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-1");

        // Assert
        var domain1 = hierarchy.GetBrokerRack(1);
        var domain2 = hierarchy.GetBrokerRack(2);
        Assert.Same(domain1, domain2);
        Assert.Contains(1, domain1!.Brokers);
        Assert.Contains(2, domain1.Brokers);
    }

    [Fact]
    public void RegisterBroker_DifferentRacksSameZone_SharesParentDomain()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");

        // Assert - Both racks share the same zone (via parent relationship)
        var rack1 = hierarchy.GetBrokerRack(1);
        var rack2 = hierarchy.GetBrokerRack(2);
        Assert.NotNull(rack1);
        Assert.NotNull(rack2);
        Assert.Same(rack1.Parent, rack2.Parent);
        Assert.Equal("zone-a", rack1.Parent!.Name);
    }

    [Fact]
    public void UnregisterBroker_RemovesBrokerFromDomain()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "rack-1");
        hierarchy.RegisterBroker(2, "rack-1");

        // Act
        hierarchy.UnregisterBroker(1);

        // Assert
        var domain1 = hierarchy.GetBrokerRack(1);
        Assert.Null(domain1);

        var domain2 = hierarchy.GetBrokerRack(2);
        Assert.NotNull(domain2);
        Assert.DoesNotContain(1, domain2.Brokers);
        Assert.Contains(2, domain2.Brokers);
    }

    [Fact]
    public void GetDomainsAtLevel_ReturnsAllDomainsAtLevel()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-b/rack-2");
        hierarchy.RegisterBroker(3, "us-west/dc2/zone-c/rack-3");

        // Act
        var zones = hierarchy.GetDomainsAtLevel(FailureDomainLevel.Zone).ToList();
        var dcs = hierarchy.GetDomainsAtLevel(FailureDomainLevel.Datacenter).ToList();
        var regions = hierarchy.GetDomainsAtLevel(FailureDomainLevel.Region).ToList();

        // Assert
        Assert.Equal(3, zones.Count);
        Assert.Equal(2, dcs.Count);
        Assert.Equal(2, regions.Count);
    }

    [Fact]
    public void CountDomainsAtLevel_ReturnsCorrectCount()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");
        hierarchy.RegisterBroker(3, "us-east/dc1/zone-b/rack-3");

        // Act & Assert
        Assert.Equal(3, hierarchy.CountDomainsAtLevel(FailureDomainLevel.Rack));
        Assert.Equal(2, hierarchy.CountDomainsAtLevel(FailureDomainLevel.Zone));
        Assert.Equal(1, hierarchy.CountDomainsAtLevel(FailureDomainLevel.Datacenter));
        Assert.Equal(1, hierarchy.CountDomainsAtLevel(FailureDomainLevel.Region));
    }

    [Fact]
    public void GetDistinctDomains_ReturnsUniqueDomainsForBrokers()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");
        hierarchy.RegisterBroker(3, "us-east/dc1/zone-b/rack-3");

        // Act - Get distinct racks (leaf level)
        var distinctRacks = hierarchy.GetDistinctDomains([1, 2, 3], FailureDomainLevel.Rack).ToList();

        // Assert - 3 distinct racks
        Assert.Equal(3, distinctRacks.Count);
    }

    [Fact]
    public void GetSiblingDomains_ReturnsSiblingsWithSameParent()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2"); // Same zone, different rack

        // Get the rack domains
        var rack1 = hierarchy.GetBrokerRack(1)!;

        // Act - Get sibling racks (same parent zone)
        var siblings = hierarchy.GetSiblingDomains(rack1).ToList();

        // Assert - rack-2 is the sibling of rack-1
        Assert.Single(siblings);
        Assert.Equal("rack-2", siblings[0].Name);
    }

    [Fact]
    public void GetBrokersInDomain_ReturnsAllBrokersInHierarchy()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");

        // Get the zone domain (parent of rack domains)
        var rack1 = hierarchy.GetBrokerRack(1)!;
        var zoneDomain = rack1.Parent!;

        // Act - Get all brokers in the zone
        var brokers = hierarchy.GetBrokersInDomain(zoneDomain).ToList();

        // Assert - Both brokers are in the zone
        Assert.Equal(2, brokers.Count);
        Assert.Contains(1, brokers);
        Assert.Contains(2, brokers);
    }

    [Fact]
    public void RootDomains_ReturnsTopLevelDomains()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-west/dc2/zone-b/rack-2");

        // Act
        var roots = hierarchy.RootDomains;

        // Assert
        Assert.Equal(2, roots.Count);
        Assert.Contains(roots, d => d.Name == "us-east");
        Assert.Contains(roots, d => d.Name == "us-west");
    }

    [Fact]
    public void Clear_RemovesAllDomainsAndBrokers()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-west/dc2/zone-b/rack-2");

        // Act
        hierarchy.Clear();

        // Assert
        Assert.Null(hierarchy.GetBrokerRack(1));
        Assert.Null(hierarchy.GetBrokerRack(2));
        Assert.Empty(hierarchy.RootDomains);
    }

    [Fact]
    public void FailureDomain_GetAllBrokers_ReturnsAllDescendantBrokers()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");

        // Get the zone domain (parent of rack domains)
        var rack1 = hierarchy.GetBrokerRack(1)!;
        var zoneDomain = rack1.Parent!;

        // Act
        var allBrokers = zoneDomain.GetAllBrokers().ToList();

        // Assert
        Assert.Equal(2, allBrokers.Count);
        Assert.Contains(1, allBrokers);
        Assert.Contains(2, allBrokers);
    }

    [Fact]
    public void FailureDomain_IsAncestorOf_ReturnsTrue()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");

        var rackDomain = hierarchy.GetBrokerRack(1)!;
        var zoneDomain = rackDomain.Parent!;

        // Act & Assert
        Assert.True(zoneDomain.IsAncestorOf(rackDomain));
        Assert.False(rackDomain.IsAncestorOf(zoneDomain));
    }

    [Fact]
    public void FailureDomain_ContainsBroker_SearchesRecursively()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        hierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");
        hierarchy.RegisterBroker(3, "us-west/dc2/zone-b/rack-3");

        // Get the zone domain (parent of rack domains for broker 1 and 2)
        var rack1 = hierarchy.GetBrokerRack(1)!;
        var zoneDomain = rack1.Parent!;

        // Act & Assert - Zone contains broker 1 and 2, but not broker 3
        Assert.True(zoneDomain.ContainsBroker(1));
        Assert.True(zoneDomain.ContainsBroker(2));
        Assert.False(zoneDomain.ContainsBroker(3));
    }

    [Fact]
    public void FailureDomain_Equality_BasedOnPath()
    {
        // Arrange
        var domain1 = new FailureDomain { Level = FailureDomainLevel.Rack, Name = "rack-1", Path = "us-east/dc1/zone-a/rack-1" };
        var domain2 = new FailureDomain { Level = FailureDomainLevel.Rack, Name = "rack-1", Path = "us-east/dc1/zone-a/rack-1" };
        var domain3 = new FailureDomain { Level = FailureDomainLevel.Rack, Name = "rack-1", Path = "us-west/dc1/zone-a/rack-1" };

        // Act & Assert
        Assert.Equal(domain1, domain2);
        Assert.NotEqual(domain1, domain3);
        Assert.Equal(domain1.GetHashCode(), domain2.GetHashCode());
    }

    [Fact]
    public void FailureDomain_ToString_ReturnsLevelAndPath()
    {
        // Arrange
        var domain = new FailureDomain { Level = FailureDomainLevel.Zone, Name = "zone-a", Path = "us-east/dc1/zone-a" };

        // Act
        var str = domain.ToString();

        // Assert
        Assert.Equal("Zone:us-east/dc1/zone-a", str);
    }

    [Fact]
    public void RegisterBroker_ReRegisterWithDifferentRack_MovesBroker()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();
        hierarchy.RegisterBroker(1, "rack-1");

        // Act
        hierarchy.RegisterBroker(1, "rack-2");

        // Assert
        var domain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(domain);
        Assert.Equal("rack-2", domain.Name);

        // Verify broker was removed from old domain
        var oldDomain = hierarchy.GetDomainsAtLevel(FailureDomainLevel.Rack)
            .FirstOrDefault(d => d.Name == "rack-1");
        if (oldDomain != null)
        {
            Assert.DoesNotContain(1, oldDomain.Brokers);
        }
    }

    [Fact]
    public void TwoLevelHierarchy_CorrectlyParsed()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act - Two level hierarchy (zone/rack)
        hierarchy.RegisterBroker(1, "zone-a/rack-1");

        // Assert
        var rackDomain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(rackDomain);
        Assert.Equal(FailureDomainLevel.Rack, rackDomain.Level);
        Assert.Equal("rack-1", rackDomain.Name);

        // Parent is zone level
        Assert.NotNull(rackDomain.Parent);
        Assert.Equal(FailureDomainLevel.Zone, rackDomain.Parent.Level);
        Assert.Equal("zone-a", rackDomain.Parent.Name);
    }

    [Fact]
    public void ThreeLevelHierarchy_CorrectlyParsed()
    {
        // Arrange
        var hierarchy = new FailureDomainHierarchy();

        // Act - Three level hierarchy (dc/zone/rack)
        hierarchy.RegisterBroker(1, "dc1/zone-a/rack-1");

        // Assert
        var rackDomain = hierarchy.GetBrokerRack(1);
        Assert.NotNull(rackDomain);
        Assert.Equal(FailureDomainLevel.Rack, rackDomain.Level);
        Assert.Equal("rack-1", rackDomain.Name);

        // Parent is zone
        Assert.NotNull(rackDomain.Parent);
        Assert.Equal(FailureDomainLevel.Zone, rackDomain.Parent.Level);
        Assert.Equal("zone-a", rackDomain.Parent.Name);

        // Grandparent is datacenter
        Assert.NotNull(rackDomain.Parent.Parent);
        Assert.Equal(FailureDomainLevel.Datacenter, rackDomain.Parent.Parent.Level);
        Assert.Equal("dc1", rackDomain.Parent.Parent.Name);
    }
}
