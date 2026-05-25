# ACL Authorization

Access Control Lists for resource authorization.

## Overview

ACLs control:
- **Who** (principal) can perform
- **What** (operation) on
- **Which** (resource)

## Configuration

```json
{
  "Surgewave": {
    "Security": {
      "AclEnabled": true,
      "SuperUsers": ["User:admin"],
      "DefaultAclAction": "Deny"
    }
  }
}
```

## CLI Usage

### List ACLs

```bash
surgewave acls list
surgewave acls list --principal User:alice
surgewave acls list --resource-type topic --resource my-topic
```

### Add ACL

```bash
# Allow user to read from topic
surgewave acls add \
    --principal User:alice \
    --resource-type topic \
    --resource my-topic \
    --operation read \
    --permission allow

# Allow user to produce to all topics
surgewave acls add \
    --principal User:producer \
    --resource-type topic \
    --resource "*" \
    --operation write \
    --permission allow

# Allow consumer group access
surgewave acls add \
    --principal User:consumer \
    --resource-type group \
    --resource my-group \
    --operation read \
    --permission allow
```

### Remove ACL

```bash
surgewave acls remove \
    --principal User:alice \
    --resource-type topic \
    --resource my-topic
```

## Resource Types

| Type | Description |
|------|-------------|
| `topic` | Topic access |
| `group` | Consumer group |
| `cluster` | Cluster operations |
| `transactional-id` | Transaction access |

## Operations

| Operation | Resources | Description |
|-----------|-----------|-------------|
| `read` | topic, group | Consume/fetch |
| `write` | topic | Produce |
| `create` | topic, group | Create new |
| `delete` | topic, group | Delete |
| `alter` | topic, group, cluster | Modify config |
| `describe` | all | View metadata |
| `all` | all | All operations |

## Pattern Types

### Literal

Exact match:

```bash
surgewave acls add \
    --principal User:app \
    --resource-type topic \
    --resource orders \
    --pattern-type literal \
    --operation read
```

### Prefixed

Prefix match:

```bash
surgewave acls add \
    --principal User:app \
    --resource-type topic \
    --resource "app." \
    --pattern-type prefixed \
    --operation all
```

Matches: `app.orders`, `app.events`, `app.logs`

## Common Patterns

### Producer

```bash
# Write to specific topics
surgewave acls add --principal User:producer \
    --resource-type topic --resource orders \
    --operation write

# Idempotent producer
surgewave acls add --principal User:producer \
    --resource-type cluster --resource kafka-cluster \
    --operation idempotent-write
```

### Consumer

```bash
# Read from topics
surgewave acls add --principal User:consumer \
    --resource-type topic --resource orders \
    --operation read

# Consumer group
surgewave acls add --principal User:consumer \
    --resource-type group --resource order-processors \
    --operation read
```

### Admin

```bash
# Create topics
surgewave acls add --principal User:admin \
    --resource-type cluster --resource kafka-cluster \
    --operation create

# Alter configs
surgewave acls add --principal User:admin \
    --resource-type cluster --resource kafka-cluster \
    --operation alter
```

### Transactional Producer

```bash
surgewave acls add --principal User:tx-producer \
    --resource-type transactional-id --resource my-tx-id \
    --operation write
```

## Super Users

Super users bypass all ACL checks:

```json
{
  "Surgewave": {
    "Security": {
      "SuperUsers": ["User:admin", "User:root"]
    }
  }
}
```

## API Access

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

await client.Admin.CreateAclsAsync(entries);
```

### Describe ACLs

```csharp
var result = await client.Admin.DescribeAclsAsync(
    resourceType: AclResourceType.Topic,
    resourceName: "my-topic");
```

## Authorization Flow

```
Request arrives
    ↓
Is user super user? → Yes → Allow
    ↓ No
Find matching ACLs
    ↓
Any DENY ACL matches? → Yes → Deny
    ↓ No
Any ALLOW ACL matches? → Yes → Allow
    ↓ No
Default action (Deny)
```

## Monitoring

| Metric | Description |
|--------|-------------|
| `surgewave_acl_denials_total` | Denied requests |
| `surgewave_acl_checks_total` | ACL checks performed |

## Best Practices

1. **Principle of least privilege** - Grant minimum required
2. **Use prefixes** - Group resources by application
3. **Super users sparingly** - Only for admin tasks
4. **Audit regularly** - Review ACL changes
5. **Deny by default** - Explicit allows only

## Next Steps

- [Clustering](../clustering/index.md) - Multi-broker security
- [Monitoring](../monitoring/index.md) - Security metrics
