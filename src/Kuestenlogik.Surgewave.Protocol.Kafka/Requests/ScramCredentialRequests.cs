namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeUserScramCredentials request (API Key 50, v0-0).
/// Describes SCRAM credentials for specified users.
/// </summary>
public sealed class DescribeUserScramCredentialsRequest : KafkaRequest
{
    /// <summary>
    /// The users to describe (null means all users).
    /// </summary>
    public List<UserName>? Users { get; init; }

    public sealed class UserName
    {
        /// <summary>The user name.</summary>
        public required string Name { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        if (Users == null)
        {
            writer.WriteVarInt(0); // Null compact array
        }
        else
        {
            writer.WriteVarInt(Users.Count + 1);
            foreach (var user in Users)
            {
                writer.WriteCompactString(user.Name);
                writer.WriteVarInt(0); // User tagged fields
            }
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeUserScramCredentialsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        List<UserName>? users = null;

        var count = reader.ReadVarInt() - 1;
        if (count >= 0)
        {
            users = new List<UserName>(count);
            for (int i = 0; i < count; i++)
            {
                users.Add(new UserName
                {
                    Name = reader.ReadCompactString() ?? ""
                });
                reader.SkipTaggedFields();
            }
        }

        reader.SkipTaggedFields();

        return new DescribeUserScramCredentialsRequest
        {
            ApiKey = ApiKey.DescribeUserScramCredentials,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Users = users
        };
    }
}

/// <summary>
/// Kafka DescribeUserScramCredentials response (API Key 50, v0-0).
/// </summary>
public sealed class DescribeUserScramCredentialsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The message-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The message-level error message.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>The results per user.</summary>
    public required List<DescribeUserScramCredentialsResult> Results { get; init; }

    public sealed class DescribeUserScramCredentialsResult
    {
        /// <summary>The user name.</summary>
        public required string User { get; init; }

        /// <summary>The user-level error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The user-level error message.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The credentials for this user.</summary>
        public required List<CredentialInfo> CredentialInfos { get; init; }
    }

    public sealed class CredentialInfo
    {
        /// <summary>
        /// The SCRAM mechanism.
        /// 0 = UNKNOWN, 1 = SCRAM-SHA-256, 2 = SCRAM-SHA-512.
        /// </summary>
        public required sbyte Mechanism { get; init; }

        /// <summary>The number of iterations used.</summary>
        public required int Iterations { get; init; }
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
            writer.WriteCompactString(result.User);
            writer.WriteInt16((short)result.ErrorCode);
            writer.WriteCompactString(result.ErrorMessage);

            writer.WriteVarInt(result.CredentialInfos.Count + 1);
            foreach (var cred in result.CredentialInfos)
            {
                writer.WriteInt8(cred.Mechanism);
                writer.WriteInt32(cred.Iterations);
                writer.WriteVarInt(0); // CredentialInfo tagged fields
            }

            writer.WriteVarInt(0); // Result tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeUserScramCredentialsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<DescribeUserScramCredentialsResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            var user = reader.ReadCompactString() ?? "";
            var userErrorCode = (ErrorCode)reader.ReadInt16();
            var userErrorMessage = reader.ReadCompactString();

            var credCount = reader.ReadVarInt() - 1;
            var credInfos = new List<CredentialInfo>(credCount);

            for (int j = 0; j < credCount; j++)
            {
                credInfos.Add(new CredentialInfo
                {
                    Mechanism = reader.ReadInt8(),
                    Iterations = reader.ReadInt32()
                });
                reader.SkipTaggedFields();
            }

            reader.SkipTaggedFields();

            results.Add(new DescribeUserScramCredentialsResult
            {
                User = user,
                ErrorCode = userErrorCode,
                ErrorMessage = userErrorMessage,
                CredentialInfos = credInfos
            });
        }

        reader.SkipTaggedFields();

        return new DescribeUserScramCredentialsResponse
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

/// <summary>
/// Kafka AlterUserScramCredentials request (API Key 51, v0-0).
/// Alters SCRAM credentials for specified users.
/// </summary>
public sealed class AlterUserScramCredentialsRequest : KafkaRequest
{
    /// <summary>The credential deletions.</summary>
    public required List<ScramCredentialDeletion> Deletions { get; init; }

    /// <summary>The credential upsertions.</summary>
    public required List<ScramCredentialUpsertion> Upsertions { get; init; }

    public sealed class ScramCredentialDeletion
    {
        /// <summary>The user name.</summary>
        public required string Name { get; init; }

        /// <summary>
        /// The SCRAM mechanism.
        /// 0 = UNKNOWN, 1 = SCRAM-SHA-256, 2 = SCRAM-SHA-512.
        /// </summary>
        public required sbyte Mechanism { get; init; }
    }

    public sealed class ScramCredentialUpsertion
    {
        /// <summary>The user name.</summary>
        public required string Name { get; init; }

        /// <summary>
        /// The SCRAM mechanism.
        /// 0 = UNKNOWN, 1 = SCRAM-SHA-256, 2 = SCRAM-SHA-512.
        /// </summary>
        public required sbyte Mechanism { get; init; }

        /// <summary>The number of iterations (must be >= 4096).</summary>
        public required int Iterations { get; init; }

        /// <summary>A random salt generated by the client.</summary>
        public required byte[] Salt { get; init; }

        /// <summary>The salted password.</summary>
        public required byte[] SaltedPassword { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteVarInt(Deletions.Count + 1);
        foreach (var deletion in Deletions)
        {
            writer.WriteCompactString(deletion.Name);
            writer.WriteInt8(deletion.Mechanism);
            writer.WriteVarInt(0); // Deletion tagged fields
        }

        writer.WriteVarInt(Upsertions.Count + 1);
        foreach (var upsertion in Upsertions)
        {
            writer.WriteCompactString(upsertion.Name);
            writer.WriteInt8(upsertion.Mechanism);
            writer.WriteInt32(upsertion.Iterations);
            writer.WriteCompactBytes(upsertion.Salt);
            writer.WriteCompactBytes(upsertion.SaltedPassword);
            writer.WriteVarInt(0); // Upsertion tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterUserScramCredentialsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var deletionCount = reader.ReadVarInt() - 1;
        var deletions = new List<ScramCredentialDeletion>(deletionCount);

        for (int i = 0; i < deletionCount; i++)
        {
            deletions.Add(new ScramCredentialDeletion
            {
                Name = reader.ReadCompactString() ?? "",
                Mechanism = reader.ReadInt8()
            });
            reader.SkipTaggedFields();
        }

        var upsertionCount = reader.ReadVarInt() - 1;
        var upsertions = new List<ScramCredentialUpsertion>(upsertionCount);

        for (int i = 0; i < upsertionCount; i++)
        {
            upsertions.Add(new ScramCredentialUpsertion
            {
                Name = reader.ReadCompactString() ?? "",
                Mechanism = reader.ReadInt8(),
                Iterations = reader.ReadInt32(),
                Salt = reader.ReadCompactBytes() ?? [],
                SaltedPassword = reader.ReadCompactBytes() ?? []
            });
            reader.SkipTaggedFields();
        }

        reader.SkipTaggedFields();

        return new AlterUserScramCredentialsRequest
        {
            ApiKey = ApiKey.AlterUserScramCredentials,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Deletions = deletions,
            Upsertions = upsertions
        };
    }
}

/// <summary>
/// Kafka AlterUserScramCredentials response (API Key 51, v0-0).
/// </summary>
public sealed class AlterUserScramCredentialsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The results for each user.</summary>
    public required List<AlterUserScramCredentialsResult> Results { get; init; }

    public sealed class AlterUserScramCredentialsResult
    {
        /// <summary>The user name.</summary>
        public required string User { get; init; }

        /// <summary>The user-level error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The user-level error message.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteCompactString(result.User);
            writer.WriteInt16((short)result.ErrorCode);
            writer.WriteCompactString(result.ErrorMessage);
            writer.WriteVarInt(0); // Result tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterUserScramCredentialsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<AlterUserScramCredentialsResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            results.Add(new AlterUserScramCredentialsResult
            {
                User = reader.ReadCompactString() ?? "",
                ErrorCode = (ErrorCode)reader.ReadInt16(),
                ErrorMessage = reader.ReadCompactString()
            });
            reader.SkipTaggedFields();
        }

        reader.SkipTaggedFields();

        return new AlterUserScramCredentialsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Results = results
        };
    }
}
