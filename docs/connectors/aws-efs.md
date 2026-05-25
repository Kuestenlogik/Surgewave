# AWS EFS Connector

The EFS connector enables monitoring and managing AWS Elastic File System resources through Surgewave.

## Overview

- **Source**: Poll EFS file systems for status changes, mount targets, and access points
- **Sink**: Create, update, and delete EFS file systems, access points, and mount targets

**Use Cases:**
- Infrastructure monitoring and alerting
- Automated EFS provisioning via event-driven workflows
- File system lifecycle management
- Compliance auditing and inventory tracking

> **Note:** This connector manages EFS resources via the AWS API. For actual file read/write operations on mounted EFS, use standard file I/O with the FileStream connector after mounting the file system.

## Quick Start

### EFS Source Connector

Monitor EFS file systems and produce status events:

```json
{
  "name": "efs-monitor",
  "config": {
    "connector.class": "EfsSourceConnector",
    "topic": "efs-events",
    "aws.region": "us-east-1",
    "aws.access.key.id": "AKIAIOSFODNN7EXAMPLE",
    "aws.secret.access.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "poll.interval.ms": "30000",
    "include.mount.targets": "true",
    "include.access.points": "true"
  }
}
```

### EFS Sink Connector

Manage EFS resources based on incoming messages:

```json
{
  "name": "efs-manager",
  "config": {
    "connector.class": "EfsSinkConnector",
    "aws.region": "us-east-1",
    "aws.access.key.id": "AKIAIOSFODNN7EXAMPLE",
    "aws.secret.access.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "topics": "efs-commands",
    "operation.field": "operation",
    "encrypted": "true",
    "performance.mode": "generalPurpose"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `aws.region` | string | `us-east-1` | AWS region |
| `aws.access.key.id` | password | - | AWS access key ID (uses default credentials if not set) |
| `aws.secret.access.key` | password | - | AWS secret access key |
| `aws.endpoint` | string | - | Custom endpoint URL (e.g., for LocalStack) |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic for EFS events |
| `poll.interval.ms` | int | `30000` | Polling interval in milliseconds |
| `file.system.ids` | string | - | Comma-separated file system IDs to monitor (empty = all) |
| `include.mount.targets` | boolean | `true` | Include mount target information in events |
| `include.access.points` | boolean | `true` | Include access point information in events |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `operation.field` | string | `operation` | Field containing the operation type |
| `file.system.id.field` | string | `file_system_id` | Field containing the file system ID |
| `name.field` | string | `name` | Field containing the resource name |
| `performance.mode` | string | `generalPurpose` | Default performance mode (`generalPurpose`, `maxIO`) |
| `throughput.mode` | string | `bursting` | Default throughput mode (`bursting`, `provisioned`, `elastic`) |
| `provisioned.throughput.mibps` | double | `0` | Provisioned throughput in MiB/s (for provisioned mode) |
| `encrypted` | boolean | `true` | Enable encryption at rest |
| `kms.key.id` | string | - | KMS key ID for encryption |
| `default.tags` | string | - | Default tags in format `key1=value1,key2=value2` |

## Source Event Format

The source connector produces JSON events with file system status:

```json
{
  "file_system_id": "fs-12345678",
  "file_system_arn": "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-12345678",
  "name": "my-efs",
  "creation_time": "2024-01-15T10:30:00Z",
  "lifecycle_state": "available",
  "size_in_bytes": 6144,
  "number_of_mount_targets": 2,
  "performance_mode": "generalPurpose",
  "throughput_mode": "bursting",
  "provisioned_throughput_mibps": 0,
  "encrypted": true,
  "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
  "tags": {
    "Name": "my-efs",
    "Environment": "production"
  },
  "mount_targets": [
    "fsmt-12345678:available",
    "fsmt-87654321:available"
  ],
  "access_points": [
    "fsap-12345678:available"
  ]
}
```

## Sink Operations

The sink connector supports the following operations:

### Create File System

```json
{
  "operation": "create_file_system",
  "name": "my-new-efs",
  "performance_mode": "generalPurpose",
  "throughput_mode": "bursting",
  "encrypted": true
}
```

### Delete File System

```json
{
  "operation": "delete_file_system",
  "file_system_id": "fs-12345678"
}
```

### Update File System

```json
{
  "operation": "update_file_system",
  "file_system_id": "fs-12345678",
  "throughput_mode": "provisioned",
  "provisioned_throughput_mibps": 100
}
```

### Create Access Point

```json
{
  "operation": "create_access_point",
  "file_system_id": "fs-12345678",
  "name": "app-data",
  "path": "/app/data",
  "owner_uid": 1000,
  "owner_gid": 1000,
  "permissions": "755",
  "posix_uid": 1000,
  "posix_gid": 1000
}
```

### Delete Access Point

```json
{
  "operation": "delete_access_point",
  "access_point_id": "fsap-12345678"
}
```

### Create Mount Target

```json
{
  "operation": "create_mount_target",
  "file_system_id": "fs-12345678",
  "subnet_id": "subnet-12345678",
  "ip_address": "10.0.1.100",
  "security_groups": ["sg-12345678", "sg-87654321"]
}
```

### Delete Mount Target

```json
{
  "operation": "delete_mount_target",
  "mount_target_id": "fsmt-12345678"
}
```

## Using EFS for File Operations

The EFS connector manages EFS resources via the AWS API. For actual file read/write operations:

1. **Mount the EFS file system** to your EC2 instances or EKS pods
2. **Use the FileStream connector** or standard file I/O to read/write files

Example with FileStream connector on mounted EFS:

```json
{
  "name": "efs-files-source",
  "config": {
    "connector.class": "FileStreamSourceConnector",
    "file": "/mnt/efs/incoming",
    "topic": "efs-files"
  }
}
```

## LocalStack Testing

For local development, use LocalStack:

```json
{
  "name": "efs-localstack",
  "config": {
    "connector.class": "EfsSourceConnector",
    "topic": "efs-events",
    "aws.region": "us-east-1",
    "aws.endpoint": "http://localhost:4566",
    "aws.access.key.id": "test",
    "aws.secret.access.key": "test"
  }
}
```

## Best Practices

1. **Use IAM roles** instead of access keys when running on EC2/EKS
2. **Enable encryption** for sensitive data at rest
3. **Monitor lifecycle states** to detect and alert on issues
4. **Use access points** to provide application-specific entry points
5. **Tag resources** for cost allocation and organization
6. **Set appropriate polling intervals** to balance responsiveness with API costs

## See Also

- [AWS S3 Connector](s3.md) - For S3 object storage
- [Custom Connectors](custom-connectors.md) - Building custom connectors
- [AWS EFS Documentation](https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html)
