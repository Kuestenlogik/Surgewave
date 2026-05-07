using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Manages a hierarchical tree of failure domains.
/// Parses broker rack strings in the format: "region/datacenter/zone/rack"
/// </summary>
public sealed class FailureDomainHierarchy
{
    private readonly Dictionary<string, FailureDomain> _domainsByPath = new();
    private readonly Dictionary<int, FailureDomain> _brokerDomains = new();
    private readonly List<FailureDomain> _rootDomains = [];

    /// <summary>
    /// Gets all root-level domains (typically regions).
    /// </summary>
    public IReadOnlyList<FailureDomain> RootDomains => _rootDomains;

    /// <summary>
    /// Registers a broker with its rack string.
    /// </summary>
    /// <param name="brokerId">The broker ID.</param>
    /// <param name="rack">The rack string in hierarchical format (e.g., "us-east/dc1/zone-a/rack-1").</param>
    public void RegisterBroker(int brokerId, string? rack)
    {
        // Remove broker from previous domain if re-registering
        if (_brokerDomains.TryGetValue(brokerId, out var oldDomain))
        {
            oldDomain.Brokers.Remove(brokerId);
        }

        var rackString = rack ?? "default";
        var domain = GetOrCreateDomain(rackString);
        domain.Brokers.Add(brokerId);
        _brokerDomains[brokerId] = domain;
    }

    /// <summary>
    /// Unregisters a broker from the hierarchy.
    /// </summary>
    public void UnregisterBroker(int brokerId)
    {
        if (_brokerDomains.TryGetValue(brokerId, out var domain))
        {
            domain.Brokers.Remove(brokerId);
            _brokerDomains.Remove(brokerId);
        }
    }

    /// <summary>
    /// Gets the failure domain for a broker at the specified level.
    /// </summary>
    public FailureDomain? GetBrokerDomain(int brokerId, FailureDomainLevel level)
    {
        if (!_brokerDomains.TryGetValue(brokerId, out var domain))
            return null;

        // Walk up to find domain at requested level
        var current = domain;
        while (current != null && current.Level > level)
        {
            current = current.Parent;
        }

        return current?.Level == level ? current : null;
    }

    /// <summary>
    /// Gets the leaf domain (rack) for a broker.
    /// </summary>
    public FailureDomain? GetBrokerRack(int brokerId)
    {
        return _brokerDomains.GetValueOrDefault(brokerId);
    }

    /// <summary>
    /// Gets all distinct domains at the specified level.
    /// </summary>
    public IEnumerable<FailureDomain> GetDomainsAtLevel(FailureDomainLevel level)
    {
        return _domainsByPath.Values.Where(d => d.Level == level);
    }

    /// <summary>
    /// Gets all brokers in a specific domain.
    /// </summary>
    public IEnumerable<int> GetBrokersInDomain(FailureDomain domain)
    {
        return domain.GetAllBrokers();
    }

    /// <summary>
    /// Gets sibling domains (domains at the same level with the same parent).
    /// </summary>
    public IEnumerable<FailureDomain> GetSiblingDomains(FailureDomain domain)
    {
        if (domain.Parent != null)
        {
            return domain.Parent.Children.Where(c => c != domain);
        }

        return _rootDomains.Where(d => d != domain);
    }

    /// <summary>
    /// Counts distinct domains at the specified level.
    /// </summary>
    public int CountDomainsAtLevel(FailureDomainLevel level)
    {
        return _domainsByPath.Values.Count(d => d.Level == level);
    }

    /// <summary>
    /// Gets distinct domain paths for a set of brokers at the specified level.
    /// </summary>
    public IEnumerable<string> GetDistinctDomains(IEnumerable<int> brokerIds, FailureDomainLevel level)
    {
        return brokerIds
            .Select(id => GetBrokerDomain(id, level))
            .Where(d => d != null)
            .Select(d => d!.Path)
            .Distinct();
    }

    /// <summary>
    /// Parses a rack string and creates/returns the domain hierarchy.
    /// </summary>
    private FailureDomain GetOrCreateDomain(string rackString)
    {
        if (_domainsByPath.TryGetValue(rackString, out var existing))
            return existing;

        var parts = rackString.Split('/');
        FailureDomain? parent = null;
        var pathBuilder = new List<string>();

        for (int i = 0; i < parts.Length; i++)
        {
            pathBuilder.Add(parts[i]);
            var path = string.Join("/", pathBuilder);
            var level = GetLevelForDepth(i, parts.Length);

            if (!_domainsByPath.TryGetValue(path, out var domain))
            {
                domain = new FailureDomain
                {
                    Level = level,
                    Name = parts[i],
                    Path = path,
                    Parent = parent
                };

                _domainsByPath[path] = domain;

                if (parent != null)
                {
                    parent.Children.Add(domain);
                }
                else
                {
                    _rootDomains.Add(domain);
                }
            }

            parent = domain;
        }

        return parent!;
    }

    /// <summary>
    /// Determines the failure domain level based on depth in hierarchy.
    /// </summary>
    private static FailureDomainLevel GetLevelForDepth(int depth, int totalDepth)
    {
        // For a 4-level hierarchy: Region(0), Datacenter(1), Zone(2), Rack(3)
        // For shorter hierarchies, we start from the bottom (Rack)
        var levelsFromBottom = totalDepth - 1 - depth;

        return levelsFromBottom switch
        {
            0 => FailureDomainLevel.Rack,
            1 => FailureDomainLevel.Zone,
            2 => FailureDomainLevel.Datacenter,
            _ => FailureDomainLevel.Region
        };
    }

    /// <summary>
    /// Clears all domains and broker registrations.
    /// </summary>
    public void Clear()
    {
        _domainsByPath.Clear();
        _brokerDomains.Clear();
        _rootDomains.Clear();
    }
}
