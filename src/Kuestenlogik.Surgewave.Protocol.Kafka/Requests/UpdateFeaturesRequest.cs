namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka UpdateFeatures request (API Key 57, v0-1).
/// Updates cluster-wide finalized features.
/// </summary>
public sealed class UpdateFeaturesRequest : KafkaRequest
{
    /// <summary>How long to wait in milliseconds before timing out the request.</summary>
    public int TimeoutMs { get; init; }

    /// <summary>The list of feature updates.</summary>
    public required List<FeatureUpdateKey> FeatureUpdates { get; init; }

    /// <summary>
    /// True if we should validate the request but not perform the upgrade or downgrade.
    /// Only available in v1+.
    /// </summary>
    public bool ValidateOnly { get; init; }

    public sealed class FeatureUpdateKey
    {
        /// <summary>The name of the finalized feature to be updated.</summary>
        public required string Feature { get; init; }

        /// <summary>
        /// The new maximum version level for the finalized feature.
        /// A value >= 1 is valid. A value of 0 returns an error.
        /// </summary>
        public required short MaxVersionLevel { get; init; }

        /// <summary>
        /// DEPRECATED in v1. When set to true, the finalized feature version level is downgraded
        /// to the provided value. In v1+, use UpgradeType instead.
        /// </summary>
        public bool AllowDowngrade { get; init; }

        /// <summary>
        /// v1+: Determines if the feature version level upgrade is safe (1) or unsafe (2),
        /// or if it's a downgrade which is always safe (3).
        /// 0 = unknown, 1 = upgrade, 2 = safe_downgrade, 3 = unsafe_downgrade.
        /// </summary>
        public sbyte UpgradeType { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(TimeoutMs);

        writer.WriteVarInt(FeatureUpdates.Count + 1);
        foreach (var update in FeatureUpdates)
        {
            writer.WriteCompactString(update.Feature);
            writer.WriteInt16(update.MaxVersionLevel);

            if (ApiVersion >= 1)
            {
                writer.WriteInt8(update.UpgradeType);
            }
            else
            {
                writer.WriteBoolean(update.AllowDowngrade);
            }

            writer.WriteVarInt(0); // Feature tagged fields
        }

        if (ApiVersion >= 1)
        {
            writer.WriteBoolean(ValidateOnly);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static UpdateFeaturesRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var timeoutMs = reader.ReadInt32();

        var updateCount = reader.ReadVarInt() - 1;
        var featureUpdates = new List<FeatureUpdateKey>(updateCount);

        for (int i = 0; i < updateCount; i++)
        {
            var feature = reader.ReadCompactString() ?? "";
            var maxVersionLevel = reader.ReadInt16();

            sbyte upgradeType = 0;
            bool allowDowngrade = false;

            if (apiVersion >= 1)
            {
                upgradeType = reader.ReadInt8();
            }
            else
            {
                allowDowngrade = reader.ReadBoolean();
            }

            reader.SkipTaggedFields();

            featureUpdates.Add(new FeatureUpdateKey
            {
                Feature = feature,
                MaxVersionLevel = maxVersionLevel,
                AllowDowngrade = allowDowngrade,
                UpgradeType = upgradeType
            });
        }

        var validateOnly = apiVersion >= 1 && reader.ReadBoolean();

        reader.SkipTaggedFields();

        return new UpdateFeaturesRequest
        {
            ApiKey = ApiKey.UpdateFeatures,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TimeoutMs = timeoutMs,
            FeatureUpdates = featureUpdates,
            ValidateOnly = validateOnly
        };
    }
}

/// <summary>
/// Kafka UpdateFeatures response (API Key 57, v0-1).
/// </summary>
public sealed class UpdateFeaturesResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top-level error code, or 0 if there was no top-level error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The top-level error message, or null if there was no top-level error.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>Results for each feature update.</summary>
    public required List<UpdatableFeatureResult> Results { get; init; }

    public sealed class UpdatableFeatureResult
    {
        /// <summary>The name of the finalized feature.</summary>
        public required string Feature { get; init; }

        /// <summary>The feature update error code, or 0 if the feature update succeeded.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The feature update error message, or null if the feature update succeeded.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteCompactString(result.Feature);
            writer.WriteInt16((short)result.ErrorCode);
            writer.WriteCompactString(result.ErrorMessage);
            writer.WriteVarInt(0); // Result tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static UpdateFeaturesResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<UpdatableFeatureResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            results.Add(new UpdatableFeatureResult
            {
                Feature = reader.ReadCompactString() ?? "",
                ErrorCode = (ErrorCode)reader.ReadInt16(),
                ErrorMessage = reader.ReadCompactString()
            });
            reader.SkipTaggedFields();
        }

        reader.SkipTaggedFields();

        return new UpdateFeaturesResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Results = results
        };
    }
}
