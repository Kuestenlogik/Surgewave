using System.Globalization;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// KIP-1240's share-group configs: their names, the values a group may set, and what a group runs with — the value it
/// set, else the broker's default (#229).
/// </summary>
internal static class ShareGroupConfigs
{
    public const string DeliveryCountLimit = "share.delivery.count.limit";
    public const string MaxRecordLocks = "share.partition.max.record.locks";
    public const string RenewAcknowledgeEnable = "share.renew.acknowledge.enable";

    /// <summary>Whether the name is one of the share-group configs.</summary>
    public static bool IsKnown(string name) => name is DeliveryCountLimit or MaxRecordLocks or RenewAcknowledgeEnable;

    /// <summary>Why the value cannot be set, null when it can.</summary>
    public static string? Validate(string name, string? value) => name switch
    {
        DeliveryCountLimit => !int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count)
            ? $"Config '{name}' must be an integer."
            : count is < 2 or > 10 ? $"Config '{name}' must be between 2 and 10 (KIP-1240 clamp)." : null,
        MaxRecordLocks => !int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var locks)
            ? $"Config '{name}' must be an integer."
            : locks is < 100 or > 10000 ? $"Config '{name}' must be between 100 and 10000 (KIP-1240 clamp)." : null,
        RenewAcknowledgeEnable => !bool.TryParse(value, out _) ? $"Config '{name}' must be 'true' or 'false'." : null,
        _ => $"Config '{name}' is not a recognized share-group config (KIP-1240).",
    };

    /// <summary>The group's limits: what it set, the broker's default for the rest.</summary>
    public static (int DeliveryLimit, int MaxRecordLocks, bool RenewEnabled) Of(
        IReadOnlyDictionary<string, string> configs, ShareGroupDefaults defaults) =>
    (
        configs.TryGetValue(DeliveryCountLimit, out var count) && int.TryParse(count, NumberStyles.Integer, CultureInfo.InvariantCulture, out var c) ? c : defaults.DeliveryCountLimit,
        configs.TryGetValue(MaxRecordLocks, out var locks) && int.TryParse(locks, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) ? l : defaults.MaxRecordLocks,
        configs.TryGetValue(RenewAcknowledgeEnable, out var renew) && bool.TryParse(renew, out var r) ? r : defaults.RenewAcknowledgeEnable
    );
}
