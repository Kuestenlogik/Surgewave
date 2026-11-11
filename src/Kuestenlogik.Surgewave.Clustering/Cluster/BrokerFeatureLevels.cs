namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// The Kafka feature levels this broker supports and runs at — the one table that ApiVersions
/// (supported and finalized features) and the broker registration read (#199). A level is
/// announced only when Surgewave has what the level gates; a client that sees a finalized level
/// uses the features behind it.
/// </summary>
public static class BrokerFeatureLevels
{
    /// <summary>
    /// <c>metadata.version</c>: supported 14–29, finalized 29.
    /// <list type="bullet">
    /// <item>14 is the floor: delegation tokens, which Surgewave has.</item>
    /// <item>22 (<c>IBP_4_0_IV0</c>) is the bootstrap level of <c>group.version</c> 1 (KIP-848
    /// consumer groups); 23 (<c>IBP_4_0_IV1</c>) the level ELR depends on; 24 (<c>IBP_4_0_IV2</c>)
    /// the bootstrap level of <c>transaction.version</c> 2 (KIP-890, #190); 28 the bootstrap level
    /// of <c>share.version</c> 1 (KIP-932 share groups). All of them are implemented.</item>
    /// <item>The other levels up to 29 change internal metadata records or inter-broker RPCs that
    /// clients do not see.</item>
    /// <item>Not higher: 30 gates cordoned log directories, 32 CIDR ACLs and 33 controller
    /// unregistration, none of which Surgewave has; 31 sits above 30, and a metadata.version
    /// includes every level below it.</item>
    /// </list>
    /// </summary>
    public static readonly FeatureLevel MetadataVersion = new("metadata.version", 14, 29, 29);

    /// <summary>
    /// <c>transaction.version</c>: supported 0–2, finalized 2. Level 2 is Transactions V2
    /// (KIP-890: an epoch bump on every EndTxn, implicit partition registration), implemented
    /// end to end in #190.
    /// </summary>
    public static readonly FeatureLevel TransactionVersion = new("transaction.version", 0, 2, 2);

    /// <summary>
    /// <c>group.version</c>: supported 0–1, finalized 1. Level 1 is the KIP-848 consumer group
    /// protocol. librdkafka 2.4+ starts its ConsumerGroupHeartbeat loop only when level 1 is both
    /// supported and finalized; without it the next-gen consumer hangs in its join state.
    /// </summary>
    public static readonly FeatureLevel GroupVersion = new("group.version", 0, 1, 1);

    /// <summary>
    /// <c>share.version</c>: supported 0–1, finalized 1. Level 1 is KIP-932 share groups. Level 2
    /// (KAFKA-20415 / KIP-1191) adds dead-letter queues for share groups, which Surgewave does not
    /// implement yet; raise the maximum once ShareGroupCoordinator has them.
    /// </summary>
    public static readonly FeatureLevel ShareVersion = new("share.version", 0, 1, 1);

    /// <summary>
    /// Every feature announced to Kafka clients, in the order ApiVersions lists them.
    /// <c>kraft.version</c>, <c>eligible.leader.replicas.version</c> and <c>streams.version</c>
    /// are not announced (<c>streams.version</c> is #239).
    /// </summary>
    public static IReadOnlyList<FeatureLevel> Announced { get; } =
        [GroupVersion, TransactionVersion, MetadataVersion, ShareVersion];

    /// <summary>
    /// Kafka's hard dependencies between feature levels that touch the features in this table or
    /// could be announced next to them. Kafka's other features (<c>transaction.version</c>,
    /// <c>group.version</c>, <c>share.version</c>) only name a bootstrap <c>metadata.version</c>,
    /// which is a default for new clusters, not a dependency. A test holds every announced level
    /// to these rules.
    /// </summary>
    public static IReadOnlyList<FeatureDependency> Dependencies { get; } =
    [
        // EligibleLeaderReplicasVersion.ELRV_1 depends on MetadataVersion.IBP_4_0_IV1 (KIP-966).
        new("eligible.leader.replicas.version", 1, MetadataVersion.Name, 23),
    ];
}
