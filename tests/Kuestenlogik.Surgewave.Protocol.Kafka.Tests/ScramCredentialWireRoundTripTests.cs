using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — KIP-554 SCRAM credential admin RPCs.
/// Covers <see cref="DescribeUserScramCredentialsRequest"/> + Response
/// (API key 50, v0) and <see cref="AlterUserScramCredentialsRequest"/>
/// + Response (API key 51, v0). Both v0+ flexible.
///
/// These RPCs back the <c>kafka-configs.sh --user … --alter --add-config
/// "SCRAM-SHA-256=[…]"</c> provisioning path and the Confluent.Kafka
/// AdminClient's <c>AlterUserScramCredentialsAsync</c>. Salt + salted
/// password travel as raw byte arrays — the wire shape needs to
/// preserve them bit-exact or authentication breaks silently.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ScramCredentialWireRoundTripTests
{
    private const sbyte ScramSha256 = 1;
    private const sbyte ScramSha512 = 2;

    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeUserScramCredentials (API key 50, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeRequest_SpecificUsers_RoundTrips()
    {
        var original = new DescribeUserScramCredentialsRequest
        {
            ApiKey = ApiKey.DescribeUserScramCredentials,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "scram-admin",
            Users =
            [
                new DescribeUserScramCredentialsRequest.UserName { Name = "alice" },
                new DescribeUserScramCredentialsRequest.UserName { Name = "bob"   },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = DescribeUserScramCredentialsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "scram-admin");

        Assert.NotNull(parsed.Users);
        Assert.Equal(2, parsed.Users!.Count);
        Assert.Equal("alice", parsed.Users[0].Name);
        Assert.Equal("bob", parsed.Users[1].Name);
    }

    [Fact]
    public void DescribeRequest_NullUsers_MeansAllUsers()
    {
        // Users=null = describe ALL users with SCRAM creds. Wire-distinct
        // from empty list — varint(0) for null-marker vs varint(1) for
        // empty (count+1 in compact-array encoding).
        var original = new DescribeUserScramCredentialsRequest
        {
            ApiKey = ApiKey.DescribeUserScramCredentials,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "scram-admin",
            Users = null,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = DescribeUserScramCredentialsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "scram-admin");

        Assert.Null(parsed.Users);
    }

    [Fact]
    public void DescribeResponse_FullShape_RoundTrips()
    {
        // Alice has both SHA-256 and SHA-512 creds; Bob has only SHA-512;
        // Carol's lookup failed with RESOURCE_NOT_FOUND (83).
        var original = new DescribeUserScramCredentialsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Results =
            [
                new DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
                {
                    User = "alice",
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                    CredentialInfos =
                    [
                        new DescribeUserScramCredentialsResponse.CredentialInfo { Mechanism = ScramSha256, Iterations = 4_096 },
                        new DescribeUserScramCredentialsResponse.CredentialInfo { Mechanism = ScramSha512, Iterations = 8_192 },
                    ],
                },
                new DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
                {
                    User = "bob",
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                    CredentialInfos =
                    [
                        new DescribeUserScramCredentialsResponse.CredentialInfo { Mechanism = ScramSha512, Iterations = 4_096 },
                    ],
                },
                new DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
                {
                    User = "carol",
                    ErrorCode = (ErrorCode)83, // RESOURCE_NOT_FOUND
                    ErrorMessage = "User does not have any SCRAM credentials",
                    CredentialInfos = [],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeUserScramCredentialsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal(3, parsed.Results.Count);

        Assert.Equal("alice", parsed.Results[0].User);
        Assert.Equal(2, parsed.Results[0].CredentialInfos.Count);
        Assert.Equal(ScramSha256, parsed.Results[0].CredentialInfos[0].Mechanism);
        Assert.Equal(4_096, parsed.Results[0].CredentialInfos[0].Iterations);
        Assert.Equal(ScramSha512, parsed.Results[0].CredentialInfos[1].Mechanism);

        Assert.Equal("bob", parsed.Results[1].User);
        Assert.Single(parsed.Results[1].CredentialInfos);

        Assert.Equal((ErrorCode)83, parsed.Results[2].ErrorCode);
        Assert.Contains("does not have", parsed.Results[2].ErrorMessage);
        Assert.Empty(parsed.Results[2].CredentialInfos);
    }

    [Fact]
    public void DescribeResponse_EmptyResults_RoundTrips()
    {
        // Cluster has no SCRAM users yet — empty Results list.
        var original = new DescribeUserScramCredentialsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Results = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeUserScramCredentialsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);
        Assert.Empty(parsed.Results);
    }

    // ───────────────────────────────────────────────────────────────
    // AlterUserScramCredentials (API key 51, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AlterRequest_UpsertOnly_RoundTrips_PreservesSaltAndSaltedPasswordBitExact()
    {
        // The salt + salted password travel as raw byte arrays via
        // WriteCompactBytes. Even a one-byte drift corrupts the
        // password hash so the user can't authenticate. Test the
        // bit-exact round-trip with bytes that span the 0x00-0xFF
        // range to catch any "treat as string" UTF-8 mangling.
        var salt = new byte[32];
        var saltedPwd = new byte[64];
        for (var i = 0; i < salt.Length; i++) salt[i] = (byte)i;
        for (var i = 0; i < saltedPwd.Length; i++) saltedPwd[i] = (byte)(0xFF - i);

        var original = new AlterUserScramCredentialsRequest
        {
            ApiKey = ApiKey.AlterUserScramCredentials,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "scram-admin",
            Deletions = [],
            Upsertions =
            [
                new AlterUserScramCredentialsRequest.ScramCredentialUpsertion
                {
                    Name = "alice",
                    Mechanism = ScramSha256,
                    Iterations = 4_096,
                    Salt = salt,
                    SaltedPassword = saltedPwd,
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterUserScramCredentialsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "scram-admin");

        Assert.Empty(parsed.Deletions);
        Assert.Single(parsed.Upsertions);
        Assert.Equal("alice", parsed.Upsertions[0].Name);
        Assert.Equal(ScramSha256, parsed.Upsertions[0].Mechanism);
        Assert.Equal(4_096, parsed.Upsertions[0].Iterations);
        Assert.Equal(salt, parsed.Upsertions[0].Salt);
        Assert.Equal(saltedPwd, parsed.Upsertions[0].SaltedPassword);
    }

    [Fact]
    public void AlterRequest_DeletionsOnly_RoundTrips()
    {
        var original = new AlterUserScramCredentialsRequest
        {
            ApiKey = ApiKey.AlterUserScramCredentials,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "scram-admin",
            Deletions =
            [
                new AlterUserScramCredentialsRequest.ScramCredentialDeletion { Name = "alice", Mechanism = ScramSha256 },
                new AlterUserScramCredentialsRequest.ScramCredentialDeletion { Name = "alice", Mechanism = ScramSha512 },
            ],
            Upsertions = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterUserScramCredentialsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "scram-admin");

        Assert.Equal(2, parsed.Deletions.Count);
        Assert.Equal("alice", parsed.Deletions[0].Name);
        Assert.Equal(ScramSha256, parsed.Deletions[0].Mechanism);
        Assert.Equal(ScramSha512, parsed.Deletions[1].Mechanism);
        Assert.Empty(parsed.Upsertions);
    }

    [Fact]
    public void AlterRequest_MixedDeletionsAndUpsertions_RoundTrips()
    {
        // Mid-cluster password rotation: delete old SHA-256, upsert
        // new SHA-512 — the most realistic real-world AlterUserScram
        // shape since SHA-256 has been deprecated for several Kafka
        // releases.
        var salt = new byte[] { 0xAB, 0xCD, 0xEF };
        var saltedPwd = new byte[] { 0x12, 0x34, 0x56, 0x78 };

        var original = new AlterUserScramCredentialsRequest
        {
            ApiKey = ApiKey.AlterUserScramCredentials,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "scram-admin",
            Deletions =
            [
                new AlterUserScramCredentialsRequest.ScramCredentialDeletion { Name = "alice", Mechanism = ScramSha256 },
            ],
            Upsertions =
            [
                new AlterUserScramCredentialsRequest.ScramCredentialUpsertion
                {
                    Name = "alice",
                    Mechanism = ScramSha512,
                    Iterations = 16_384,
                    Salt = salt,
                    SaltedPassword = saltedPwd,
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterUserScramCredentialsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "scram-admin");

        Assert.Single(parsed.Deletions);
        Assert.Single(parsed.Upsertions);
        Assert.Equal(16_384, parsed.Upsertions[0].Iterations);
        Assert.Equal(salt, parsed.Upsertions[0].Salt);
        Assert.Equal(saltedPwd, parsed.Upsertions[0].SaltedPassword);
    }

    [Fact]
    public void AlterResponse_FullShape_PerUserResults_RoundTrips()
    {
        var original = new AlterUserScramCredentialsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results =
            [
                new AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
                {
                    User = "alice",
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                },
                new AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
                {
                    User = "bob",
                    ErrorCode = (ErrorCode)82, // UNACCEPTABLE_CREDENTIAL — iterations < 4096
                    ErrorMessage = "Iterations 1024 below minimum 4096",
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterUserScramCredentialsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(2, parsed.Results.Count);
        Assert.Equal("alice", parsed.Results[0].User);
        Assert.Equal(ErrorCode.None, parsed.Results[0].ErrorCode);
        Assert.Null(parsed.Results[0].ErrorMessage);
        Assert.Equal("bob", parsed.Results[1].User);
        Assert.Equal((ErrorCode)82, parsed.Results[1].ErrorCode);
        Assert.Contains("below minimum", parsed.Results[1].ErrorMessage);
    }

    [Fact]
    public void AlterResponse_EmptyResults_RoundTrips()
    {
        var original = new AlterUserScramCredentialsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterUserScramCredentialsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);
        Assert.Empty(parsed.Results);
    }
}
