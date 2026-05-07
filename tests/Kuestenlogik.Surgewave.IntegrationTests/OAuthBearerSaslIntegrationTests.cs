using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// End-to-end OAUTHBEARER (KIP-936) authentication: an in-process IdP signs a
/// JWT, Confluent.Kafka presents it via the <c>OAuthBearerTokenRefreshHandler</c>
/// callback, and the broker validates the signature against the IdP's JWKS
/// document fetched over HTTP. Closes the unit-test gap left by the wiring
/// suite — until this lands the JWKS path was only exercised by the unit
/// stub.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection("OAuthBearerBroker")]
public sealed class OAuthBearerSaslIntegrationTests
{
    private const int MessageCount = 5;
    private readonly OAuthBearerBrokerFixture _fixture;

    public OAuthBearerSaslIntegrationTests(OAuthBearerBrokerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ValidJwt_ProducerCanSendAndConsumerCanReceive()
    {
        var topic = $"oauthbearer-rt-{Guid.NewGuid():N}".Substring(0, 32);
        var groupId = $"oauthbearer-grp-{Guid.NewGuid():N}".Substring(0, 32);

        var producerCfg = BaseConfig(new ProducerConfig
        {
            BootstrapServers = OAuthBearerBrokerFixture.BootstrapServers,
            Acks = Acks.All,
            MessageTimeoutMs = 10_000,
        });

        using var producer = new ProducerBuilder<string, string>(producerCfg)
            .SetOAuthBearerTokenRefreshHandler(RefreshHandler)
            .Build();

        for (var i = 0; i < MessageCount; i++)
        {
            var result = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"k-{i}",
                Value = $"oauth-msg-{i}",
            });
            Assert.Equal(PersistenceStatus.Persisted, result.Status);
        }
        producer.Flush(TimeSpan.FromSeconds(10));

        var consumerCfg = BaseConfig(new ConsumerConfig
        {
            BootstrapServers = OAuthBearerBrokerFixture.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        });

        using var consumer = new ConsumerBuilder<string, string>(consumerCfg)
            .SetOAuthBearerTokenRefreshHandler(RefreshHandler)
            .Build();
        consumer.Subscribe(topic);

        var received = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (received.Count < MessageCount && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr?.Message is not null) received.Add(cr.Message.Value);
        }
        consumer.Close();

        Assert.Equal(MessageCount, received.Count);
    }

    [Fact]
    public async Task WrongIssuer_ConnectionFails()
    {
        // Broker is configured with ValidIssuer = TestIssuer; signing the same
        // token with a different issuer must be rejected by JwksTokenValidator,
        // even though the signature itself is valid against the JWKS.
        var producerCfg = BaseConfig(new ProducerConfig
        {
            BootstrapServers = OAuthBearerBrokerFixture.BootstrapServers,
            MessageTimeoutMs = 5_000,
            SocketTimeoutMs = 4_000,
        });

        using var producer = new ProducerBuilder<string, string>(producerCfg)
            .SetOAuthBearerTokenRefreshHandler((c, _) =>
            {
                var token = _fixture.MintToken(issuer: "https://attacker.example");
                c.OAuthBearerSetToken(
                    tokenValue: token,
                    lifetimeMs: DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds(),
                    principalName: OAuthBearerBrokerFixture.TestSubject);
            })
            .Build();

        await Assert.ThrowsAsync<ProduceException<string, string>>(async () =>
        {
            await producer.ProduceAsync("oauth-wrong-issuer", new Message<string, string>
            {
                Key = "k", Value = "should-fail",
            });
        });
    }

    private void RefreshHandler(IProducer<string, string> client, string _)
        => SetFreshToken(client.OAuthBearerSetToken, client.OAuthBearerSetTokenFailure);

    private void RefreshHandler(IConsumer<string, string> client, string _)
        => SetFreshToken(client.OAuthBearerSetToken, client.OAuthBearerSetTokenFailure);

    private void SetFreshToken(
        Action<string, long, string, IDictionary<string, string>?> setToken,
        Action<string> setFailure)
    {
        try
        {
            var token = _fixture.MintToken();
            var lifetimeMs = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
            setToken(token, lifetimeMs, OAuthBearerBrokerFixture.TestSubject, null);
        }
        catch (Exception ex)
        {
            setFailure(ex.Message);
        }
    }

    private static T BaseConfig<T>(T cfg) where T : ClientConfig
    {
        cfg.SecurityProtocol = SecurityProtocol.SaslPlaintext;
        cfg.SaslMechanism = SaslMechanism.OAuthBearer;
        // librdkafka requires this even when the refresh handler supplies tokens
        // — without it the client refuses to start the OAUTHBEARER state machine.
        cfg.SaslOauthbearerConfig = "principal=" + OAuthBearerBrokerFixture.TestSubject;
        return cfg;
    }
}
