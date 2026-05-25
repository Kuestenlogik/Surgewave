using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Broker.Security;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Maps between proto/gRPC ACL enum values and internal broker ACL enums.
/// Proto uses different numbering than internal enums for historical reasons.
/// </summary>
public static class AclEnumMapper
{
    // Proto AclResourceType: RESOURCE_ANY=0, ACL_TOPIC=1, ACL_GROUP=2, ACL_CLUSTER=3, TRANSACTIONAL_ID=4, DELEGATION_TOKEN=5
    // Internal AclResourceType: Unknown=0, Any=1, Topic=2, Group=3, Cluster=4, TransactionalId=5, DelegationToken=6
    public static AclResourceType MapProtoToInternalResourceType(int protoValue) => protoValue switch
    {
        0 => AclResourceType.Any,
        1 => AclResourceType.Topic,
        2 => AclResourceType.Group,
        3 => AclResourceType.Cluster,
        4 => AclResourceType.TransactionalId,
        5 => AclResourceType.DelegationToken,
        _ => AclResourceType.Unknown
    };

    public static int MapInternalToProtoResourceType(AclResourceType internalValue) => internalValue switch
    {
        AclResourceType.Any => 0,
        AclResourceType.Topic => 1,
        AclResourceType.Group => 2,
        AclResourceType.Cluster => 3,
        AclResourceType.TransactionalId => 4,
        AclResourceType.DelegationToken => 5,
        _ => 0
    };

    // Proto AclPatternType: PATTERN_ANY=0, MATCH=1, LITERAL=2, PREFIXED=3
    // Internal AclPatternType: Unknown=0, Any=1, Match=2, Literal=3, Prefixed=4
    public static AclPatternType MapProtoToInternalPatternType(int protoValue) => protoValue switch
    {
        0 => AclPatternType.Any,
        1 => AclPatternType.Match,
        2 => AclPatternType.Literal,
        3 => AclPatternType.Prefixed,
        _ => AclPatternType.Unknown
    };

    public static int MapInternalToProtoPatternType(AclPatternType internalValue) => internalValue switch
    {
        AclPatternType.Any => 0,
        AclPatternType.Match => 1,
        AclPatternType.Literal => 2,
        AclPatternType.Prefixed => 3,
        _ => 0
    };

    // Proto AclOperation: OPERATION_ANY=0, ALL=1, READ=2, WRITE=3, CREATE=4, DELETE=5, ALTER=6, DESCRIBE=7, CLUSTER_ACTION=8, DESCRIBE_CONFIGS=9, ALTER_CONFIGS=10, IDEMPOTENT_WRITE=11
    // Internal AclOperation: Unknown=0, Any=1, All=2, Read=3, Write=4, Create=5, Delete=6, Alter=7, Describe=8, ClusterAction=9, DescribeConfigs=10, AlterConfigs=11, IdempotentWrite=12
    public static AclOperation MapProtoToInternalOperation(int protoValue) => protoValue switch
    {
        0 => AclOperation.Any,
        1 => AclOperation.All,
        2 => AclOperation.Read,
        3 => AclOperation.Write,
        4 => AclOperation.Create,
        5 => AclOperation.Delete,
        6 => AclOperation.Alter,
        7 => AclOperation.Describe,
        8 => AclOperation.ClusterAction,
        9 => AclOperation.DescribeConfigs,
        10 => AclOperation.AlterConfigs,
        11 => AclOperation.IdempotentWrite,
        _ => AclOperation.Unknown
    };

    public static int MapInternalToProtoOperation(AclOperation internalValue) => internalValue switch
    {
        AclOperation.Any => 0,
        AclOperation.All => 1,
        AclOperation.Read => 2,
        AclOperation.Write => 3,
        AclOperation.Create => 4,
        AclOperation.Delete => 5,
        AclOperation.Alter => 6,
        AclOperation.Describe => 7,
        AclOperation.ClusterAction => 8,
        AclOperation.DescribeConfigs => 9,
        AclOperation.AlterConfigs => 10,
        AclOperation.IdempotentWrite => 11,
        _ => 0
    };

    // Proto AclPermission: PERMISSION_ANY=0, ALLOW=1, DENY=2
    // Internal AclPermission: Unknown=0, Any=1, Deny=2, Allow=3
    public static AclPermission MapProtoToInternalPermission(int protoValue) => protoValue switch
    {
        0 => AclPermission.Any,
        1 => AclPermission.Allow,
        2 => AclPermission.Deny,
        _ => AclPermission.Unknown
    };

    public static int MapInternalToProtoPermission(AclPermission internalValue) => internalValue switch
    {
        AclPermission.Any => 0,
        AclPermission.Allow => 1,
        AclPermission.Deny => 2,
        _ => 0
    };

    /// <summary>
    /// Creates an ACL filter predicate from an AclBindingDto.
    /// </summary>
    public static Func<AclEntry, bool> CreateAclFilter(AclBindingDto filter)
    {
        var filterResourceType = MapProtoToInternalResourceType(filter.ResourceType);
        var filterPatternType = MapProtoToInternalPatternType(filter.PatternType);
        var filterOperation = MapProtoToInternalOperation(filter.Operation);
        var filterPermission = MapProtoToInternalPermission(filter.Permission);

        return acl =>
        {
            if (filterResourceType != AclResourceType.Any && acl.ResourceType != filterResourceType)
                return false;
            if (!string.IsNullOrEmpty(filter.ResourceName) && filter.ResourceName != "*" && acl.ResourceName != filter.ResourceName)
                return false;
            if (filterPatternType != AclPatternType.Any && acl.PatternType != filterPatternType)
                return false;
            if (!string.IsNullOrEmpty(filter.Principal) && filter.Principal != "*" && acl.Principal != filter.Principal)
                return false;
            if (!string.IsNullOrEmpty(filter.Host) && filter.Host != "*" && acl.Host != filter.Host)
                return false;
            if (filterOperation != AclOperation.Any && acl.Operation != filterOperation)
                return false;
            if (filterPermission != AclPermission.Any && acl.Permission != filterPermission)
                return false;
            return true;
        };
    }

    /// <summary>
    /// Converts an AclEntry to AclBindingDto.
    /// </summary>
    public static AclBindingDto ConvertToAclBindingDto(AclEntry acl) => new(
        MapInternalToProtoResourceType(acl.ResourceType),
        acl.ResourceName,
        MapInternalToProtoPatternType(acl.PatternType),
        acl.Principal,
        acl.Host,
        MapInternalToProtoOperation(acl.Operation),
        MapInternalToProtoPermission(acl.Permission));
}
