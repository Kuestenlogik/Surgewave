using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-554 SCRAM credential admin RPCs (DescribeUserScramCredentials,
/// AlterUserScramCredentials). Tests run with both stores (SHA256 + SHA512)
/// pre-populated so we can verify upsert / delete / describe round-trips
/// without touching disk or running the SASL handshake.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ScramCredentialsAdminTests
{
    private const sbyte MechanismScramSha256 = 1;

    [Fact]
    public async Task DescribeUserScramCredentials_NoStores_AllUsersReturnResourceNotFound()
    {
        var handler = BuildHandler(sha256: null, sha512: null);

        var resp = (DescribeUserScramCredentialsResponse)await handler.HandleAsync(
            new DescribeUserScramCredentialsRequest
            {
                ApiKey = ApiKey.DescribeUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Users = [new DescribeUserScramCredentialsRequest.UserName { Name = "alice" }],
            },
            BuildContext(),
            CancellationToken.None);

        var entry = Assert.Single(resp.Results);
        Assert.Equal("alice", entry.User);
        Assert.Equal(ErrorCode.ResourceNotFound, entry.ErrorCode);
        Assert.Empty(entry.CredentialInfos);
    }

    [Fact]
    public async Task AlterUserScramCredentials_Upsert_ThenDescribe_RoundTrips()
    {
        var sha256 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        var handler = BuildHandler(sha256: sha256, sha512: null);

        var alterResp = (AlterUserScramCredentialsResponse)await handler.HandleAsync(
            new AlterUserScramCredentialsRequest
            {
                ApiKey = ApiKey.AlterUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Deletions = [],
                Upsertions =
                [
                    new AlterUserScramCredentialsRequest.ScramCredentialUpsertion
                    {
                        Name = "bob",
                        Mechanism = MechanismScramSha256,
                        Iterations = 4096,
                        Salt = new byte[16],
                        SaltedPassword = new byte[32],
                    },
                ],
            },
            BuildContext(),
            CancellationToken.None);

        var alter = Assert.Single(alterResp.Results);
        Assert.Equal("bob", alter.User);
        Assert.Equal(ErrorCode.None, alter.ErrorCode);

        // Now describe back.
        var descResp = (DescribeUserScramCredentialsResponse)await handler.HandleAsync(
            new DescribeUserScramCredentialsRequest
            {
                ApiKey = ApiKey.DescribeUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 2,
                ClientId = "admin",
                Users = [new DescribeUserScramCredentialsRequest.UserName { Name = "bob" }],
            },
            BuildContext(),
            CancellationToken.None);

        var describe = Assert.Single(descResp.Results);
        Assert.Equal("bob", describe.User);
        Assert.Equal(ErrorCode.None, describe.ErrorCode);
        var info = Assert.Single(describe.CredentialInfos);
        Assert.Equal(MechanismScramSha256, info.Mechanism);
        Assert.Equal(4096, info.Iterations);
    }

    [Fact]
    public async Task AlterUserScramCredentials_Delete_RemovesFromStore()
    {
        var sha256 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        sha256.AddUser("carol", "hunter2");
        var handler = BuildHandler(sha256: sha256, sha512: null);

        var resp = (AlterUserScramCredentialsResponse)await handler.HandleAsync(
            new AlterUserScramCredentialsRequest
            {
                ApiKey = ApiKey.AlterUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Deletions =
                [
                    new AlterUserScramCredentialsRequest.ScramCredentialDeletion
                    {
                        Name = "carol",
                        Mechanism = MechanismScramSha256,
                    },
                ],
                Upsertions = [],
            },
            BuildContext(),
            CancellationToken.None);

        var entry = Assert.Single(resp.Results);
        Assert.Equal(ErrorCode.None, entry.ErrorCode);
        Assert.False(sha256.UserExists("carol"));
    }

    [Fact]
    public async Task AlterUserScramCredentials_UnknownMechanism_RejectedPerRow()
    {
        var sha256 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        var handler = BuildHandler(sha256: sha256, sha512: null);

        var resp = (AlterUserScramCredentialsResponse)await handler.HandleAsync(
            new AlterUserScramCredentialsRequest
            {
                ApiKey = ApiKey.AlterUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Deletions =
                [
                    new AlterUserScramCredentialsRequest.ScramCredentialDeletion
                    {
                        Name = "alice",
                        Mechanism = 99, // unknown
                    },
                ],
                Upsertions = [],
            },
            BuildContext(),
            CancellationToken.None);

        var entry = Assert.Single(resp.Results);
        Assert.Equal(ErrorCode.UnsupportedSaslMechanism, entry.ErrorCode);
    }

    [Fact]
    public async Task DescribeUserScramCredentials_NullUserList_EnumeratesAllStoredUsers()
    {
        var sha256 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        var sha512 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA512);
        sha256.AddUser("alice", "p256");
        sha512.AddUser("bob", "p512");
        var handler = BuildHandler(sha256: sha256, sha512: sha512);

        var resp = (DescribeUserScramCredentialsResponse)await handler.HandleAsync(
            new DescribeUserScramCredentialsRequest
            {
                ApiKey = ApiKey.DescribeUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Users = null, // enumerate everything
            },
            BuildContext(),
            CancellationToken.None);

        var ids = resp.Results.Select(r => r.User).OrderBy(s => s).ToList();
        Assert.Equal(["alice", "bob"], ids);
    }

    [Fact]
    public async Task AlterUserScramCredentials_InvalidShape_RejectedWithInvalidRequest()
    {
        var sha256 = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        var handler = BuildHandler(sha256: sha256, sha512: null);

        var resp = (AlterUserScramCredentialsResponse)await handler.HandleAsync(
            new AlterUserScramCredentialsRequest
            {
                ApiKey = ApiKey.AlterUserScramCredentials,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Deletions = [],
                Upsertions =
                [
                    new AlterUserScramCredentialsRequest.ScramCredentialUpsertion
                    {
                        Name = "bad-user",
                        Mechanism = MechanismScramSha256,
                        Iterations = 0, // invalid
                        Salt = new byte[16],
                        SaltedPassword = new byte[32],
                    },
                ],
            },
            BuildContext(),
            CancellationToken.None);

        var entry = Assert.Single(resp.Results);
        Assert.Equal(ErrorCode.InvalidRequest, entry.ErrorCode);
    }

    private static SecurityApiHandler BuildHandler(ScramCredentialStore? sha256, ScramCredentialStore? sha512) =>
        new(
            new BrokerConfig { BrokerId = 1 },
            saslAuthenticator: null,
            aclAuthorizer: null,
            auditLogger: null,
            NullLogger<SecurityApiHandler>.Instance,
            scramSha256Store: sha256,
            scramSha512Store: sha512);

    private static RequestContext BuildContext() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "admin",
    };
}
