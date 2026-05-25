namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka CreateDelegationToken request (API Key 38, v0-3).
/// Creates a delegation token for authentication delegation.
/// </summary>
public sealed class CreateDelegationTokenRequest : KafkaRequest
{
    /// <summary>A list of resource owners for which delegation tokens will be requested.</summary>
    public List<CreatableRenewers>? Renewers { get; init; }

    /// <summary>The maximum lifetime of the token in milliseconds (-1 for server default).</summary>
    public long MaxLifetimeMs { get; init; } = -1;

    /// <summary>The principal type of the owner of the token (v3+).</summary>
    public string? OwnerPrincipalType { get; init; }

    /// <summary>The principal name of the owner of the token (v3+).</summary>
    public string? OwnerPrincipalName { get; init; }

    public sealed class CreatableRenewers
    {
        /// <summary>The principal type of the renewer.</summary>
        public required string PrincipalType { get; init; }

        /// <summary>The principal name of the renewer.</summary>
        public required string PrincipalName { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields

            // Renewers array
            if (Renewers == null)
            {
                writer.WriteVarInt(0);
            }
            else
            {
                writer.WriteVarInt(Renewers.Count + 1);
                foreach (var renewer in Renewers)
                {
                    writer.WriteCompactString(renewer.PrincipalType);
                    writer.WriteCompactString(renewer.PrincipalName);
                    writer.WriteVarInt(0); // Renewer tagged fields
                }
            }

            writer.WriteInt64(MaxLifetimeMs);

            // v3+ owner fields
            if (ApiVersion >= 3)
            {
                writer.WriteCompactString(OwnerPrincipalType);
                writer.WriteCompactString(OwnerPrincipalName);
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            if (Renewers == null)
            {
                writer.WriteInt32(0);
            }
            else
            {
                writer.WriteInt32(Renewers.Count);
                foreach (var renewer in Renewers)
                {
                    writer.WriteString(renewer.PrincipalType);
                    writer.WriteString(renewer.PrincipalName);
                }
            }

            writer.WriteInt64(MaxLifetimeMs);
        }
    }

    public static CreateDelegationTokenRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var renewers = new List<CreatableRenewers>();

        if (isFlexible)
        {
            var renewerCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < renewerCount; i++)
            {
                renewers.Add(new CreatableRenewers
                {
                    PrincipalType = reader.ReadCompactString() ?? "",
                    PrincipalName = reader.ReadCompactString() ?? ""
                });
                reader.SkipTaggedFields();
            }
        }
        else
        {
            var renewerCount = reader.ReadInt32();
            for (int i = 0; i < renewerCount; i++)
            {
                renewers.Add(new CreatableRenewers
                {
                    PrincipalType = reader.ReadString() ?? "",
                    PrincipalName = reader.ReadString() ?? ""
                });
            }
        }

        var maxLifetimeMs = reader.ReadInt64();

        string? ownerPrincipalType = null;
        string? ownerPrincipalName = null;

        if (apiVersion >= 3)
        {
            ownerPrincipalType = reader.ReadCompactString();
            ownerPrincipalName = reader.ReadCompactString();
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new CreateDelegationTokenRequest
        {
            ApiKey = ApiKey.CreateDelegationToken,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Renewers = renewers,
            MaxLifetimeMs = maxLifetimeMs,
            OwnerPrincipalType = ownerPrincipalType,
            OwnerPrincipalName = ownerPrincipalName
        };
    }
}

/// <summary>
/// Kafka CreateDelegationToken response (API Key 38, v0-3).
/// </summary>
public sealed class CreateDelegationTokenResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The principal type of the token owner.</summary>
    public required string PrincipalType { get; init; }

    /// <summary>The principal name of the token owner.</summary>
    public required string PrincipalName { get; init; }

    /// <summary>The principal type of the requester (v3+).</summary>
    public string? TokenRequesterPrincipalType { get; init; }

    /// <summary>The principal name of the requester (v3+).</summary>
    public string? TokenRequesterPrincipalName { get; init; }

    /// <summary>When this token was generated (Unix epoch timestamp in ms).</summary>
    public long IssueTimestampMs { get; init; }

    /// <summary>When this token will expire (Unix epoch timestamp in ms).</summary>
    public long ExpiryTimestampMs { get; init; }

    /// <summary>The maximum lifetime of this token (Unix epoch timestamp in ms).</summary>
    public long MaxTimestampMs { get; init; }

    /// <summary>The token UUID.</summary>
    public required string TokenId { get; init; }

    /// <summary>HMAC of the delegation token.</summary>
    public required byte[] Hmac { get; init; }

    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteCompactString(PrincipalType);
            writer.WriteCompactString(PrincipalName);

            if (ApiVersion >= 3)
            {
                writer.WriteCompactString(TokenRequesterPrincipalType);
                writer.WriteCompactString(TokenRequesterPrincipalName);
            }

            writer.WriteInt64(IssueTimestampMs);
            writer.WriteInt64(ExpiryTimestampMs);
            writer.WriteInt64(MaxTimestampMs);
            writer.WriteCompactString(TokenId);
            writer.WriteCompactBytes(Hmac);
            writer.WriteInt32(ThrottleTimeMs);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(PrincipalType);
            writer.WriteString(PrincipalName);
            writer.WriteInt64(IssueTimestampMs);
            writer.WriteInt64(ExpiryTimestampMs);
            writer.WriteInt64(MaxTimestampMs);
            writer.WriteString(TokenId);
            writer.WriteBytes(Hmac);
            writer.WriteInt32(ThrottleTimeMs);
        }
    }

    public static CreateDelegationTokenResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        string principalType, principalName;
        string? tokenRequesterPrincipalType = null, tokenRequesterPrincipalName = null;

        if (isFlexible)
        {
            principalType = reader.ReadCompactString() ?? "";
            principalName = reader.ReadCompactString() ?? "";

            if (apiVersion >= 3)
            {
                tokenRequesterPrincipalType = reader.ReadCompactString();
                tokenRequesterPrincipalName = reader.ReadCompactString();
            }
        }
        else
        {
            principalType = reader.ReadString() ?? "";
            principalName = reader.ReadString() ?? "";
        }

        var issueTimestampMs = reader.ReadInt64();
        var expiryTimestampMs = reader.ReadInt64();
        var maxTimestampMs = reader.ReadInt64();

        string tokenId;
        byte[] hmac;

        if (isFlexible)
        {
            tokenId = reader.ReadCompactString() ?? "";
            hmac = reader.ReadCompactBytes() ?? [];
        }
        else
        {
            tokenId = reader.ReadString() ?? "";
            hmac = reader.ReadBytes() ?? [];
        }

        var throttleTimeMs = reader.ReadInt32();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new CreateDelegationTokenResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            PrincipalType = principalType,
            PrincipalName = principalName,
            TokenRequesterPrincipalType = tokenRequesterPrincipalType,
            TokenRequesterPrincipalName = tokenRequesterPrincipalName,
            IssueTimestampMs = issueTimestampMs,
            ExpiryTimestampMs = expiryTimestampMs,
            MaxTimestampMs = maxTimestampMs,
            TokenId = tokenId,
            Hmac = hmac,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}

/// <summary>
/// Kafka RenewDelegationToken request (API Key 39, v0-2).
/// Renews a delegation token to extend its expiry time.
/// </summary>
public sealed class RenewDelegationTokenRequest : KafkaRequest
{
    /// <summary>The HMAC of the delegation token to renew.</summary>
    public required byte[] Hmac { get; init; }

    /// <summary>The renewal time period in milliseconds.</summary>
    public long RenewPeriodMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
            writer.WriteCompactBytes(Hmac);
            writer.WriteInt64(RenewPeriodMs);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);
            writer.WriteBytes(Hmac);
            writer.WriteInt64(RenewPeriodMs);
        }
    }

    public static RenewDelegationTokenRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;

        byte[] hmac;
        if (isFlexible)
        {
            hmac = reader.ReadCompactBytes() ?? [];
        }
        else
        {
            hmac = reader.ReadBytes() ?? [];
        }

        var renewPeriodMs = reader.ReadInt64();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new RenewDelegationTokenRequest
        {
            ApiKey = ApiKey.RenewDelegationToken,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Hmac = hmac,
            RenewPeriodMs = renewPeriodMs
        };
    }
}

/// <summary>
/// Kafka RenewDelegationToken response (API Key 39, v0-2).
/// </summary>
public sealed class RenewDelegationTokenResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The timestamp in ms at which this token expires.</summary>
    public long ExpiryTimestampMs { get; init; }

    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt64(ExpiryTimestampMs);
        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static RenewDelegationTokenResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var errorCode = (ErrorCode)reader.ReadInt16();
        var expiryTimestampMs = reader.ReadInt64();
        var throttleTimeMs = reader.ReadInt32();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new RenewDelegationTokenResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            ExpiryTimestampMs = expiryTimestampMs,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}

/// <summary>
/// Kafka ExpireDelegationToken request (API Key 40, v0-2).
/// Expires a delegation token.
/// </summary>
public sealed class ExpireDelegationTokenRequest : KafkaRequest
{
    /// <summary>The HMAC of the delegation token to expire.</summary>
    public required byte[] Hmac { get; init; }

    /// <summary>
    /// The expiry time period in milliseconds.
    /// A negative value means expire immediately.
    /// </summary>
    public long ExpiryTimePeriodMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
            writer.WriteCompactBytes(Hmac);
            writer.WriteInt64(ExpiryTimePeriodMs);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);
            writer.WriteBytes(Hmac);
            writer.WriteInt64(ExpiryTimePeriodMs);
        }
    }

    public static ExpireDelegationTokenRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;

        byte[] hmac;
        if (isFlexible)
        {
            hmac = reader.ReadCompactBytes() ?? [];
        }
        else
        {
            hmac = reader.ReadBytes() ?? [];
        }

        var expiryTimePeriodMs = reader.ReadInt64();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new ExpireDelegationTokenRequest
        {
            ApiKey = ApiKey.ExpireDelegationToken,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Hmac = hmac,
            ExpiryTimePeriodMs = expiryTimePeriodMs
        };
    }
}

/// <summary>
/// Kafka ExpireDelegationToken response (API Key 40, v0-2).
/// </summary>
public sealed class ExpireDelegationTokenResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The timestamp in ms at which this token expires.</summary>
    public long ExpiryTimestampMs { get; init; }

    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt64(ExpiryTimestampMs);
        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static ExpireDelegationTokenResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var errorCode = (ErrorCode)reader.ReadInt16();
        var expiryTimestampMs = reader.ReadInt64();
        var throttleTimeMs = reader.ReadInt32();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new ExpireDelegationTokenResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            ExpiryTimestampMs = expiryTimestampMs,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}

/// <summary>
/// Kafka DescribeDelegationToken request (API Key 41, v0-3).
/// Describes delegation tokens for specified owners.
/// </summary>
public sealed class DescribeDelegationTokenRequest : KafkaRequest
{
    /// <summary>
    /// The owners to describe (null means all tokens).
    /// </summary>
    public List<DescribeDelegationTokenOwner>? Owners { get; init; }

    public sealed class DescribeDelegationTokenOwner
    {
        /// <summary>The principal type of the owner.</summary>
        public required string PrincipalType { get; init; }

        /// <summary>The principal name of the owner.</summary>
        public required string PrincipalName { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields

            if (Owners == null)
            {
                writer.WriteVarInt(0); // Null compact array
            }
            else
            {
                writer.WriteVarInt(Owners.Count + 1);
                foreach (var owner in Owners)
                {
                    writer.WriteCompactString(owner.PrincipalType);
                    writer.WriteCompactString(owner.PrincipalName);
                    writer.WriteVarInt(0); // Owner tagged fields
                }
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            if (Owners == null)
            {
                writer.WriteInt32(-1); // Null array
            }
            else
            {
                writer.WriteInt32(Owners.Count);
                foreach (var owner in Owners)
                {
                    writer.WriteString(owner.PrincipalType);
                    writer.WriteString(owner.PrincipalName);
                }
            }
        }
    }

    public static DescribeDelegationTokenRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        List<DescribeDelegationTokenOwner>? owners = null;

        if (isFlexible)
        {
            var count = reader.ReadVarInt() - 1;
            if (count >= 0)
            {
                owners = new List<DescribeDelegationTokenOwner>(count);
                for (int i = 0; i < count; i++)
                {
                    owners.Add(new DescribeDelegationTokenOwner
                    {
                        PrincipalType = reader.ReadCompactString() ?? "",
                        PrincipalName = reader.ReadCompactString() ?? ""
                    });
                    reader.SkipTaggedFields();
                }
            }
            reader.SkipTaggedFields();
        }
        else
        {
            var count = reader.ReadInt32();
            if (count >= 0)
            {
                owners = new List<DescribeDelegationTokenOwner>(count);
                for (int i = 0; i < count; i++)
                {
                    owners.Add(new DescribeDelegationTokenOwner
                    {
                        PrincipalType = reader.ReadString() ?? "",
                        PrincipalName = reader.ReadString() ?? ""
                    });
                }
            }
        }

        return new DescribeDelegationTokenRequest
        {
            ApiKey = ApiKey.DescribeDelegationToken,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Owners = owners
        };
    }
}

/// <summary>
/// Kafka DescribeDelegationToken response (API Key 41, v0-3).
/// </summary>
public sealed class DescribeDelegationTokenResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The tokens.</summary>
    public required List<DescribedDelegationToken> Tokens { get; init; }

    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    public sealed class DescribedDelegationToken
    {
        /// <summary>The principal type of the token owner.</summary>
        public required string PrincipalType { get; init; }

        /// <summary>The principal name of the token owner.</summary>
        public required string PrincipalName { get; init; }

        /// <summary>The principal type of the token requester (v3+).</summary>
        public string? TokenRequesterPrincipalType { get; init; }

        /// <summary>The principal name of the token requester (v3+).</summary>
        public string? TokenRequesterPrincipalName { get; init; }

        /// <summary>When this token was generated.</summary>
        public long IssueTimestampMs { get; init; }

        /// <summary>When this token will expire.</summary>
        public long ExpiryTimestampMs { get; init; }

        /// <summary>The maximum lifetime of this token.</summary>
        public long MaxTimestampMs { get; init; }

        /// <summary>The token UUID.</summary>
        public required string TokenId { get; init; }

        /// <summary>The token HMAC.</summary>
        public required byte[] Hmac { get; init; }

        /// <summary>The renewers for this token.</summary>
        public required List<DescribedDelegationTokenRenewer> Renewers { get; init; }
    }

    public sealed class DescribedDelegationTokenRenewer
    {
        /// <summary>The principal type of the renewer.</summary>
        public required string PrincipalType { get; init; }

        /// <summary>The principal name of the renewer.</summary>
        public required string PrincipalName { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(Tokens.Count + 1);
            foreach (var token in Tokens)
            {
                writer.WriteCompactString(token.PrincipalType);
                writer.WriteCompactString(token.PrincipalName);

                if (ApiVersion >= 3)
                {
                    writer.WriteCompactString(token.TokenRequesterPrincipalType);
                    writer.WriteCompactString(token.TokenRequesterPrincipalName);
                }

                writer.WriteInt64(token.IssueTimestampMs);
                writer.WriteInt64(token.ExpiryTimestampMs);
                writer.WriteInt64(token.MaxTimestampMs);
                writer.WriteCompactString(token.TokenId);
                writer.WriteCompactBytes(token.Hmac);

                writer.WriteVarInt(token.Renewers.Count + 1);
                foreach (var renewer in token.Renewers)
                {
                    writer.WriteCompactString(renewer.PrincipalType);
                    writer.WriteCompactString(renewer.PrincipalName);
                    writer.WriteVarInt(0); // Renewer tagged fields
                }

                writer.WriteVarInt(0); // Token tagged fields
            }

            writer.WriteInt32(ThrottleTimeMs);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Tokens.Count);
            foreach (var token in Tokens)
            {
                writer.WriteString(token.PrincipalType);
                writer.WriteString(token.PrincipalName);
                writer.WriteInt64(token.IssueTimestampMs);
                writer.WriteInt64(token.ExpiryTimestampMs);
                writer.WriteInt64(token.MaxTimestampMs);
                writer.WriteString(token.TokenId);
                writer.WriteBytes(token.Hmac);

                writer.WriteInt32(token.Renewers.Count);
                foreach (var renewer in token.Renewers)
                {
                    writer.WriteString(renewer.PrincipalType);
                    writer.WriteString(renewer.PrincipalName);
                }
            }

            writer.WriteInt32(ThrottleTimeMs);
        }
    }

    public static DescribeDelegationTokenResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var errorCode = (ErrorCode)reader.ReadInt16();
        var tokens = new List<DescribedDelegationToken>();

        if (isFlexible)
        {
            var tokenCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < tokenCount; i++)
            {
                var principalType = reader.ReadCompactString() ?? "";
                var principalName = reader.ReadCompactString() ?? "";

                string? tokenRequesterPrincipalType = null;
                string? tokenRequesterPrincipalName = null;

                if (apiVersion >= 3)
                {
                    tokenRequesterPrincipalType = reader.ReadCompactString();
                    tokenRequesterPrincipalName = reader.ReadCompactString();
                }

                var issueTimestampMs = reader.ReadInt64();
                var expiryTimestampMs = reader.ReadInt64();
                var maxTimestampMs = reader.ReadInt64();
                var tokenId = reader.ReadCompactString() ?? "";
                var hmac = reader.ReadCompactBytes() ?? [];

                var renewerCount = reader.ReadVarInt() - 1;
                var renewers = new List<DescribedDelegationTokenRenewer>(renewerCount);
                for (int j = 0; j < renewerCount; j++)
                {
                    renewers.Add(new DescribedDelegationTokenRenewer
                    {
                        PrincipalType = reader.ReadCompactString() ?? "",
                        PrincipalName = reader.ReadCompactString() ?? ""
                    });
                    reader.SkipTaggedFields();
                }

                reader.SkipTaggedFields();

                tokens.Add(new DescribedDelegationToken
                {
                    PrincipalType = principalType,
                    PrincipalName = principalName,
                    TokenRequesterPrincipalType = tokenRequesterPrincipalType,
                    TokenRequesterPrincipalName = tokenRequesterPrincipalName,
                    IssueTimestampMs = issueTimestampMs,
                    ExpiryTimestampMs = expiryTimestampMs,
                    MaxTimestampMs = maxTimestampMs,
                    TokenId = tokenId,
                    Hmac = hmac,
                    Renewers = renewers
                });
            }
        }
        else
        {
            var tokenCount = reader.ReadInt32();
            for (int i = 0; i < tokenCount; i++)
            {
                var principalType = reader.ReadString() ?? "";
                var principalName = reader.ReadString() ?? "";
                var issueTimestampMs = reader.ReadInt64();
                var expiryTimestampMs = reader.ReadInt64();
                var maxTimestampMs = reader.ReadInt64();
                var tokenId = reader.ReadString() ?? "";
                var hmac = reader.ReadBytes() ?? [];

                var renewerCount = reader.ReadInt32();
                var renewers = new List<DescribedDelegationTokenRenewer>(renewerCount);
                for (int j = 0; j < renewerCount; j++)
                {
                    renewers.Add(new DescribedDelegationTokenRenewer
                    {
                        PrincipalType = reader.ReadString() ?? "",
                        PrincipalName = reader.ReadString() ?? ""
                    });
                }

                tokens.Add(new DescribedDelegationToken
                {
                    PrincipalType = principalType,
                    PrincipalName = principalName,
                    IssueTimestampMs = issueTimestampMs,
                    ExpiryTimestampMs = expiryTimestampMs,
                    MaxTimestampMs = maxTimestampMs,
                    TokenId = tokenId,
                    Hmac = hmac,
                    Renewers = renewers
                });
            }
        }

        var throttleTimeMs = reader.ReadInt32();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new DescribeDelegationTokenResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            Tokens = tokens,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}
