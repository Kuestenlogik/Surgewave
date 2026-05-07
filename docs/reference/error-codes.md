# Error Codes Reference

Surgewave has two error code systems: the **Surgewave Native protocol** (used by the Surgewave .NET client) and the **Kafka protocol** (used by Confluent.Kafka and other Kafka-compatible clients).

---

## Surgewave Native Error Codes

Enum: `Kuestenlogik.Surgewave.Protocol.Native.SurgewaveErrorCode` (type: `ushort`)

### General

| Code | Name | When It Occurs |
|------|------|----------------|
| `0` | `None` | Success — no error |
| `1` | `UnknownError` | Unexpected internal broker error |
| `2` | `InvalidRequest` | Malformed request (bad fields, missing required data) |
| `3` | `TopicNotFound` | Topic does not exist and auto-create is disabled |
| `4` | `PartitionNotFound` | Partition index out of range |
| `5` | `NotLeader` | Broker is not the leader for this partition |
| `6` | `AuthenticationFailed` | SASL/TLS authentication failure |
| `7` | `AuthorizationFailed` | ACL denied the operation |
| `8` | `InvalidOffset` | Offset is out of range for the partition |
| `9` | `MessageTooLarge` | Message exceeds `MaxRequestSize` |
| `10` | `GroupNotFound` | Consumer group does not exist |
| `11` | `RebalanceInProgress` | Consumer group is currently rebalancing |
| `12` | `InvalidSession` | Session token is invalid or expired |
| `13` | `Timeout` | Operation timed out on the broker |
| `14` | `MemberIdRequired` | Member ID must be provided to rejoin group |
| `15` | `UnknownMemberId` | Member ID not recognized by the group coordinator |
| `16` | `IllegalGeneration` | Consumer group generation ID mismatch |
| `17` | `InconsistentGroupProtocol` | Members disagree on group protocol |
| `18` | `GroupNotEmpty` | Cannot delete a non-empty consumer group |
| `19` | `GroupAuthorizationFailed` | ACL denied group operation |
| `20` | `NotCoordinator` | This broker is not the group coordinator |
| `21` | `CoordinatorNotAvailable` | Group coordinator is not yet available |

### Transaction Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `30` | `InvalidProducerEpoch` | Producer epoch is stale (new instance started) |
| `31` | `UnknownProducerId` | Producer ID not recognized |
| `32` | `InvalidTxnState` | Operation not valid for current transaction state |
| `33` | `TransactionAborted` | Transaction was aborted |
| `34` | `ConcurrentTransactions` | Another transaction is in progress for this producer |
| `35` | `TransactionTimeout` | Transaction exceeded `DefaultTimeoutMs` |
| `36` | `DuplicateSequenceNumber` | Idempotent producer detected a duplicate (already committed) |
| `37` | `OutOfOrderSequenceNumber` | Producer sequence number is out of order |

### Security Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `40` | `SecurityDisabled` | Security feature required but not enabled |
| `41` | `InvalidAclFilter` | ACL filter has invalid fields |
| `42` | `AclNotFound` | ACL entry to delete was not found |

### Configuration Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `50` | `InvalidConfig` | Configuration value is invalid or out of range |
| `51` | `ConfigNotFound` | Requested configuration key does not exist |

### Leader Election Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `60` | `ElectionNotNeeded` | Preferred leader is already the current leader |
| `61` | `PreferredLeaderNotAvailable` | Preferred leader replica is not in-sync |
| `62` | `EligibleLeadersNotAvailable` | No eligible leader candidates available |

### Schema Registry Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `70` | `SchemaNotFound` | Schema ID does not exist |
| `71` | `SubjectNotFound` | Subject name does not exist |
| `72` | `VersionNotFound` | Version number not found for subject |
| `73` | `IncompatibleSchema` | Schema fails compatibility check |
| `74` | `InvalidSchema` | Schema is not valid for its declared type |
| `75` | `SchemaRegistryDisabled` | Schema Registry not enabled (`Surgewave:SchemaRegistry:Enabled=false`) |

### Connect Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `80` | `ConnectorNotFound` | Connector name does not exist |
| `81` | `ConnectorAlreadyExists` | Connector with that name already registered |
| `82` | `TaskNotFound` | Task ID not found for connector |
| `83` | `InvalidConnectorConfig` | Connector configuration validation failed |
| `84` | `ConnectDisabled` | Connect not enabled (`Surgewave:Connect:Enabled=false`) |
| `85` | `ConnectorFailed` | Connector is in a failed state |

### Plugin Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `90` | `PluginManagerDisabled` | Plugin subsystem not enabled |
| `91` | `PluginNotFound` | Plugin ID not found |
| `92` | `PluginAlreadyInstalled` | Plugin with this ID already loaded |
| `93` | `PluginInstallFailed` | Plugin load or initialization failed |
| `94` | `PluginUninstallFailed` | Plugin unload failed (e.g., still running) |
| `95` | `DependencyResolutionFailed` | Plugin dependency could not be resolved |

### Cross-Topic Transaction Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `100` | `CrossTopicTxnNotFound` | Transaction ID does not exist |
| `101` | `CrossTopicTxnInvalidState` | Operation not valid for current transaction state |
| `102` | `CrossTopicTxnTimedOut` | Transaction timed out before commit |
| `103` | `CrossTopicTxnMaxWritesExceeded` | Too many writes in a single transaction |
| `104` | `CrossTopicTxnCommitFailed` | Commit failed (partial write or broker error) |
| `105` | `CrossTopicTxnDisabled` | Feature disabled (`Surgewave:CrossTopicTransactions:Enabled=false`) |

### Streaming Subscription Errors

| Code | Name | When It Occurs |
|------|------|----------------|
| `110` | `SubscriptionAlreadyExists` | Push subscription already registered |
| `111` | `SubscriptionNotFound` | Subscription ID not found |
| `112` | `MaxSubscriptionsExceeded` | Exceeded `MaxStreamingSubscriptionsPerConnection` |

---

## Kafka Protocol Error Codes

Enum: `Kuestenlogik.Surgewave.Protocol.Kafka.ErrorCode` (type: `short`)
Used by Confluent.Kafka, librdkafka, and all Kafka-compatible clients.

| Code | Name | Notes |
|------|------|-------|
| `0` | `None` | Success |
| `-1` | `Unknown` | Unexpected error |
| `1` | `OffsetOutOfRange` | Requested offset is out of range |
| `2` | `CorruptMessage` | CRC validation failed |
| `3` | `UnknownTopicOrPartition` | Topic/partition not found |
| `4` | `InvalidFetchSize` | Fetch size too small for message |
| `5` | `LeaderNotAvailable` | Leader election in progress |
| `6` | `NotLeaderForPartition` | This broker is not partition leader |
| `7` | `RequestTimedOut` | Request deadline exceeded |
| `8` | `BrokerNotAvailable` | Broker unavailable |
| `9` | `ReplicaNotAvailable` | Replica not yet available |
| `10` | `MessageTooLarge` | Message too large for broker |
| `11` | `StaleControllerEpoch` | Controller epoch is stale |
| `12` | `OffsetMetadataTooLarge` | Offset commit metadata too large |
| `13` | `NetworkException` | Network-level error |
| `14` | `CoordinatorLoadInProgress` | Coordinator loading offset data |
| `15` | `CoordinatorNotAvailable` | Group coordinator not available |
| `16` | `NotCoordinator` | Not the group coordinator |
| `17` | `InvalidTopicException` | Topic name is invalid |
| `18` | `RecordListTooLarge` | Record batch exceeds max size |
| `19` | `NotEnoughReplicas` | ISR count below `MinInSyncReplicas` |
| `20` | `NotEnoughReplicasAfterAppend` | Write succeeded but replicas fell below min |
| `21` | `InvalidRequiredAcks` | `acks` value is invalid |
| `22` | `IllegalGeneration` | Consumer group generation mismatch |
| `23` | `InconsistentGroupProtocol` | Member protocol incompatible |
| `24` | `InvalidGroupId` | Group ID is empty or invalid |
| `25` | `UnknownMemberId` | Member ID not known to coordinator |
| `26` | `InvalidSessionTimeout` | Session timeout out of range |
| `27` | `RebalanceInProgress` | Group is currently rebalancing |
| `28` | `InvalidCommitOffsetSize` | Offset commit metadata too large |
| `29` | `TopicAuthorizationFailed` | ACL denied topic operation |
| `30` | `GroupAuthorizationFailed` | ACL denied group operation |
| `31` | `ClusterAuthorizationFailed` | ACL denied cluster-level operation |
| `32` | `InvalidTimestamp` | Record timestamp is out of range |
| `33` | `UnsupportedSaslMechanism` | SASL mechanism not supported |
| `34` | `IllegalSaslState` | SASL handshake state machine error |
| `35` | `UnsupportedVersion` | API version not supported |
| `36` | `TopicAlreadyExists` | Topic already exists |
| `37` | `InvalidPartitions` | Invalid partition count |
| `38` | `InvalidReplicationFactor` | Invalid replication factor |
| `39` | `InvalidReplicaAssignment` | Invalid replica assignment |
| `40` | `InvalidConfig` | Invalid topic or broker configuration |
| `41` | `NotController` | Broker is not the cluster controller |
| `44` | `OutOfOrderSequenceNumber` | Producer sequence is out of order |
| `46` | `DuplicateSequenceNumber` | Duplicate sequence (idempotent producer) |
| `47` | `InvalidProducerEpoch` | Producer epoch is stale |
| `48` | `InvalidTxnState` | Transaction state machine error |
| `49` | `InvalidProducerIdMapping` | Producer ID mapping mismatch |
| `50` | `InvalidTransactionTimeout` | Transaction timeout out of configured range |
| `51` | `ConcurrentTransactions` | Concurrent transaction conflict |
| `52` | `TransactionCoordinatorFenced` | Transaction coordinator has been replaced |
| `53` | `TransactionalIdAuthorizationFailed` | ACL denied transactional ID |
| `54` | `SecurityDisabled` | Security not enabled |
| `55` | `OperationNotAttempted` | Operation skipped (batch partial failure) |
| `56` | `KafkaStorageError` | Disk/storage error |
| `57` | `LogDirNotFound` | Log directory not found |
| `58` | `SaslAuthenticationFailed` | SASL credential failure |
| `59` | `UnknownProducerId` | Producer ID not found |
| `60` | `ReassignmentInProgress` | Partition reassignment in progress |
| `61` | `DelegationTokenAuthDisabled` | Delegation token auth not enabled |
| `62` | `DelegationTokenNotFound` | Token not found |
| `63` | `DelegationTokenOwnerMismatch` | Token owner mismatch |
| `64` | `DelegationTokenRequestNotAllowed` | Token request not permitted |
| `65` | `DelegationTokenAuthorizationFailed` | ACL denied token operation |
| `66` | `DelegationTokenExpired` | Token has expired |
| `67` | `InvalidPrincipalType` | Principal type not supported |
| `68` | `NonEmptyGroup` | Cannot delete non-empty group |
| `76` | `UnsupportedCompressionType` | Compression codec not supported |
| `77` | `StaleBrokerEpoch` | Broker epoch is stale |
| `87` | `SnapshotNotFound` | KRaft snapshot not found |
| `100` | `UnknownTopicId` | Topic ID (UUID) not found |
| `104` | `InconsistentClusterId` | Cluster ID mismatch |

---

## Confluent.Kafka Mapping

When using the Confluent.Kafka client against Surgewave, error codes flow through `Confluent.Kafka.Error` with the same numeric values as the Kafka protocol column above. The `Confluent.Kafka.ErrorCode` enum is a pass-through of the Kafka wire protocol codes.

```csharp
try
{
    await producer.ProduceAsync("topic", message);
}
catch (ProduceException<string, string> ex)
{
    switch (ex.Error.Code)
    {
        case ErrorCode.NotLeaderForPartition:
            // Retry with metadata refresh
            break;
        case ErrorCode.MessageTooLarge:
            // Split or compress
            break;
        case ErrorCode.TopicAuthorizationFailed:
            // Check ACLs
            break;
    }
}
```
