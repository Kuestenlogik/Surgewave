# Admin Operations

Administrative operations for topics, groups, and cluster management.

All operations below use the `SurgewaveNativeClient` which provides domain-specific operation groups: `Topics`, `Cluster`, `Groups`, `Admin`, `Schema`, `Connect`, and `Transactions`.

## Topic Management

### Create Topic

```csharp
await client.Topics.CreateAsync("new-topic",
    partitions: 3,
    replicationFactor: 1);

// With configuration using fluent builder
await client.Topics.Create("configured-topic")
    .WithPartitions(6)
    .WithReplicationFactor(3)
    .WithConfig("retention.ms", "604800000")  // 7 days
    .WithConfig("cleanup.policy", "delete")
    .ExecuteAsync();

// Ephemeral topic with ring-buffer (no persistence)
await client.Topics.Create("sensor-data")
    .WithPartitions(1)
    .WithConfig("cleanup.policy", "ephemeral")
    .WithConfig("ephemeral.buffer.bytes", "67108864")  // 64 MB ring buffer
    .ExecuteAsync();
```

### List Topics

```csharp
var topics = await client.Topics.ListAsync();
foreach (var topic in topics)
{
    Console.WriteLine($"{topic.Name} ({topic.PartitionCount} partitions)");
}
```

### Describe Topic

```csharp
var info = await client.Topics.DescribeAsync("my-topic");
Console.WriteLine($"Topic: {info.Name}");
Console.WriteLine($"Partitions: {info.PartitionCount}");
foreach (var partition in info.Partitions)
{
    Console.WriteLine($"  P{partition.Id}: Leader={partition.Leader}");
}

// Get topic configuration
var config = await client.Topics.DescribeConfigAsync("my-topic");
foreach (var (key, value) in config)
    Console.WriteLine($"  {key} = {value}");
```

### Delete Topic

```csharp
await client.Topics.DeleteAsync("old-topic");
```

### Alter Topic Config

```csharp
await client.Topics.AlterConfigAsync("my-topic", new Dictionary<string, string>
{
    ["retention.ms"] = "172800000"  // 2 days
});
```

## Consumer Group Management

### List Groups

```csharp
var groups = await client.Groups.ListAsync();
foreach (var group in groups)
{
    Console.WriteLine($"{group.GroupId}: {group.State}");
}
```

### Describe Group

```csharp
var group = await client.Groups.DescribeAsync("my-group");
Console.WriteLine($"Group: {group.GroupId}");
Console.WriteLine($"State: {group.State}");
foreach (var member in group.Members)
{
    Console.WriteLine($"  Member: {member.MemberId}");
    Console.WriteLine($"  Client: {member.ClientId}");
    Console.WriteLine($"  Partitions: {string.Join(",", member.Assignments)}");
}
```

### Delete Group

```csharp
await client.Groups.DeleteAsync("inactive-group");
```

## Cluster Operations

### Get Cluster Info

```csharp
var cluster = await client.Cluster.GetClusterInfoAsync();
Console.WriteLine($"Cluster ID: {cluster.ClusterId}");
Console.WriteLine($"Controller: {cluster.ControllerId}");
foreach (var broker in cluster.Brokers)
{
    Console.WriteLine($"  Broker {broker.Id}: {broker.Host}:{broker.Port}");
}
```

### Describe Broker Config

```csharp
var config = await client.Admin.DescribeBrokerConfigAsync(brokerId: 1);
foreach (var entry in config)
{
    Console.WriteLine($"{entry.Name}: {entry.Value}");
}
```

## ACL Management

### Describe ACLs

```csharp
var result = await client.Admin.DescribeAclsAsync(
    resourceType: AclResourceType.Topic,
    resourceName: "my-topic");

foreach (var acl in result.Entries)
{
    Console.WriteLine($"{acl.Principal} {acl.Permission} {acl.Operation} on {acl.ResourceName}");
}
```

### Create ACLs

```csharp
var entries = new List<AclEntry>
{
    new()
    {
        ResourceType = AclResourceType.Topic,
        ResourceName = "my-topic",
        PatternType = AclPatternType.Literal,
        Principal = "User:alice",
        Host = "*",
        Operation = AclOperation.Read,
        Permission = AclPermission.Allow
    }
};

var results = await client.Admin.CreateAclsAsync(entries);
```

### Delete ACLs

```csharp
var result = await client.Admin.DeleteAclsAsync(
    resourceType: AclResourceType.Topic,
    resourceName: "my-topic",
    principal: "User:alice");
```

## Partition Operations

### Elect Leader

```csharp
// Leader election
var results = await client.Admin.ElectLeaderAsync(
    ElectionType.Preferred,
    new Dictionary<string, List<int>>
    {
        ["my-topic"] = [0, 1, 2]
    });
```

### Reassign Partitions

```csharp
var reassignments = new List<PartitionReassignmentRequest>
{
    new("my-topic", 0, [1, 2, 3])
};
await client.Cluster.AlterPartitionReassignmentsAsync(reassignments);
```

## Quota Management

### Get Quotas

```csharp
var quota = await client.Admin.GetQuotaConfigAsync();
Console.WriteLine($"Produce rate: {quota.ProduceRateLimit} bytes/sec");
Console.WriteLine($"Fetch rate: {quota.FetchRateLimit} bytes/sec");
Console.WriteLine($"Enabled: {quota.Enabled}");
```

### Set Quotas

```csharp
await client.Admin.SetQuotaConfigAsync(
    produceRateLimit: 10 * 1024 * 1024,   // 10 MB/s
    fetchRateLimit: 50 * 1024 * 1024,     // 50 MB/s
    enabled: true);
```

## Error Handling

```csharp
try
{
    await client.Topics.CreateAsync("existing-topic", 3, 1);
}
catch (ProtocolException ex)
{
    Console.WriteLine($"Operation failed: {ex.ErrorCode}");
}
catch (InvalidOperationException ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}
```

## CLI Equivalents

| API Method | CLI Command |
|------------|-------------|
| `client.Topics.CreateAsync()` | `surgewave topics create` |
| `client.Topics.ListAsync()` | `surgewave topics list` |
| `client.Topics.DescribeAsync()` | `surgewave topics describe` |
| `client.Topics.DeleteAsync()` | `surgewave topics delete` |
| `client.Groups.ListAsync()` | `surgewave groups list` |
| `client.Groups.DescribeAsync()` | `surgewave groups describe` |
| `client.Admin.DescribeAclsAsync()` | `surgewave acls list` |

## Next Steps

- [Security](../security/index.md) - ACL configuration
- [Clustering](../clustering/index.md) - Multi-broker operations
- [CLI Reference](../tools/cli-reference.md) - Command-line tools
