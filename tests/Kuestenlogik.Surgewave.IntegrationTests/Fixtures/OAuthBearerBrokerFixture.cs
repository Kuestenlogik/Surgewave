using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// Fixture that stands up an in-process OIDC-style IdP (a single-route HttpListener
/// serving a JWKS document) and an embedded Surgewave broker configured with
/// SASL/OAUTHBEARER pointing at it. Tests can <see cref="MintToken"/> to produce
/// freshly-signed JWTs and then exercise the broker over the Kafka wire — no
/// external IdP, no docker, no admin privileges.
/// </summary>
#pragma warning disable CA2213
public sealed class OAuthBearerBrokerFixture : IAsyncLifetime, IDisposable
{
    public const string TestIssuer = "https://idp.test.surgewave";
    public const string TestAudience = "surgewave-broker";
    public const string TestSubject = "alice@example.com";
    private const string KeyId = "surgewave-test-rsa-1";

    private SurgewaveBroker? _broker;
    private LogManager? _logManager;
    private OffsetStore? _offsetStore;
    private TransactionStateStore? _transactionStateStore;
    private BrokerMetrics? _metrics;
    private QuotaManager? _quotaManager;
    private CancellationTokenSource? _cts;
    private Task? _brokerTask;
    private HttpListener? _jwksListener;
    private CancellationTokenSource? _jwksCts;
    private Task? _jwksTask;
    private RSA? _signingKey;
    private HttpClient? _oauthHttp;
    private string? _dataDirectory;
    private readonly ILoggerFactory _loggerFactory;
    private int _brokerPort;
    private int _jwksPort;

    private static OAuthBearerBrokerFixture? _instance;

    public static string BootstrapServers => _instance != null
        ? $"localhost:{_instance._brokerPort}"
        : throw new InvalidOperationException("OAUTHBEARER broker not started");

    public string JwksUri => $"http://localhost:{_jwksPort}/jwks";
    public string OidcAuthority => $"http://localhost:{_jwksPort}";

    public OAuthBearerBrokerFixture()
    {
        _loggerFactory = LoggerFactory.Create(b =>
        {
            b.SetMinimumLevel(LogLevel.Warning);
            b.AddConsole();
        });
    }

    public async ValueTask InitializeAsync()
    {
        _dataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-oauthbearer-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDirectory);

        _signingKey = RSA.Create(2048);
        StartJwksEndpoint();
        await WaitForJwksReady();

        _brokerPort = FindAvailablePort();
        await StartBroker(_brokerPort);
        await WaitForBrokerReady(_brokerPort);

        _instance = this;
    }

    /// <summary>
    /// Mint a freshly-signed JWT against the fixture's RSA key. Tests use this in
    /// the Confluent.Kafka <c>OAuthBearerTokenRefreshHandler</c> callback so the
    /// client never has to talk to a real IdP.
    /// </summary>
    public string MintToken(
        string? issuer = null,
        string? audience = null,
        string? subject = null,
        TimeSpan? lifetime = null)
    {
        if (_signingKey is null)
            throw new InvalidOperationException("Fixture not initialized");

        var now = DateTime.UtcNow;
        var exp = now + (lifetime ?? TimeSpan.FromMinutes(10));

        var key = new RsaSecurityKey(_signingKey) { KeyId = KeyId };
        var creds = new SigningCredentials(key, SecurityAlgorithms.RsaSha256);

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = issuer ?? TestIssuer,
            Audience = audience ?? TestAudience,
            IssuedAt = now,
            NotBefore = now,
            Expires = exp,
            Subject = new System.Security.Claims.ClaimsIdentity(
            [
                new System.Security.Claims.Claim("sub", subject ?? TestSubject)
            ]),
            SigningCredentials = creds,
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    private void StartJwksEndpoint()
    {
        _jwksPort = FindAvailablePort();
        _jwksListener = new HttpListener();
        _jwksListener.Prefixes.Add($"http://localhost:{_jwksPort}/");
        _jwksListener.Start();

        _jwksCts = new CancellationTokenSource();
        var token = _jwksCts.Token;
        _jwksTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                HttpListenerContext ctx;
                try
                {
                    ctx = await _jwksListener.GetContextAsync().ConfigureAwait(false);
                }
                catch (HttpListenerException) { break; }
                catch (ObjectDisposedException) { break; }

                try
                {
                    var path = ctx.Request.Url?.AbsolutePath;
                    string? body = path switch
                    {
                        "/jwks" => BuildJwksJson(),
                        "/.well-known/openid-configuration" => BuildOidcDiscoveryJson(),
                        _ => null,
                    };

                    if (body is not null)
                    {
                        var bytes = Encoding.UTF8.GetBytes(body);
                        ctx.Response.ContentType = "application/json";
                        ctx.Response.StatusCode = 200;
                        ctx.Response.ContentLength64 = bytes.Length;
                        await ctx.Response.OutputStream.WriteAsync(bytes, token).ConfigureAwait(false);
                    }
                    else
                    {
                        ctx.Response.StatusCode = 404;
                    }
                }
                finally
                {
                    ctx.Response.Close();
                }
            }
        }, token);
    }

    private string BuildOidcDiscoveryJson()
    {
        var doc = new
        {
            issuer = TestIssuer,
            jwks_uri = JwksUri,
            id_token_signing_alg_values_supported = new[] { "RS256" },
            response_types_supported = new[] { "id_token" },
            subject_types_supported = new[] { "public" },
        };
        return JsonSerializer.Serialize(doc);
    }

    private string BuildJwksJson()
    {
        var rsaParams = _signingKey!.ExportParameters(includePrivateParameters: false);
        var n = Base64UrlEncoder.Encode(rsaParams.Modulus!);
        var e = Base64UrlEncoder.Encode(rsaParams.Exponent!);

        var doc = new
        {
            keys = new[]
            {
                new
                {
                    kty = "RSA",
                    use = "sig",
                    alg = "RS256",
                    kid = KeyId,
                    n,
                    e,
                }
            }
        };
        return JsonSerializer.Serialize(doc);
    }

    private async Task WaitForJwksReady(int maxRetries = 30)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var resp = await http.GetAsync(JwksUri);
                if (resp.IsSuccessStatusCode) return;
            }
            catch { /* retry */ }
            await Task.Delay(100);
        }
        throw new InvalidOperationException("JWKS endpoint did not become ready");
    }

    private async Task StartBroker(int port)
    {
        var config = new BrokerConfig
        {
            Host = "localhost",
            Port = port,
            DataDirectory = _dataDirectory!,
            AutoCreateTopics = true,
            DefaultNumPartitions = 3,
            ShutdownTimeoutSeconds = 5,
            Security = new SecurityConfig
            {
                SaslEnabled = true,
                SaslMechanisms = ["OAUTHBEARER"],
                OAuthBearer = new OAuthBearerConfig
                {
                    Enabled = true,
                    OidcAuthority = OidcAuthority,
                    ValidIssuer = TestIssuer,
                    ValidAudiences = [TestAudience],
                    PrincipalClaim = "sub",
                    JwksRefreshInterval = TimeSpan.FromMinutes(5),
                    // In-process IdP fixture serves http://localhost — production
                    // deployments must keep RequireHttpsMetadata=true (the default).
                    RequireHttpsMetadata = false,
                }
            }
        };

        var retentionPolicy = new RetentionPolicy { RetentionHours = 1, RetentionBytes = -1 };
        _logManager = new LogManager(_dataDirectory!, FileLogSegmentFactory.Create(), retentionPolicy: retentionPolicy);

        var brokerLogger = _loggerFactory.CreateLogger<SurgewaveBroker>();
        var serializerLogger = _loggerFactory.CreateLogger<RecordBatchSerializer>();
        var coordinatorLogger = _loggerFactory.CreateLogger<ConsumerGroupCoordinator>();
        var txnCoordinatorLogger = _loggerFactory.CreateLogger<TransactionCoordinator>();
        var txnStateStoreLogger = _loggerFactory.CreateLogger<TransactionStateStore>();
        var offsetStoreLogger = _loggerFactory.CreateLogger<OffsetStore>();
        var jwksLogger = _loggerFactory.CreateLogger<JwksTokenValidator>();

        var recordBatchSerializer = new RecordBatchSerializer(serializerLogger);
        _offsetStore = new OffsetStore(_dataDirectory!, offsetStoreLogger);
        var consumerGroupCoordinator = new ConsumerGroupCoordinator(coordinatorLogger, _offsetStore);
        var queueViewConfig = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewConfig();
        var queueViewManager = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewManager(queueViewConfig, _loggerFactory, _logManager);
        var shareGroupCoordinator = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator>(), _logManager, queueViewManager);
        var consumerGroupV2Coordinator = new Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator>(), _logManager);
        var streamsGroupCoordinator = new Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator>(), _logManager);
        var nativeGroupCoordinatorLogger = _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.Native.NativeGroupCoordinator>();
        var nativeGroupCoordinator = new Kuestenlogik.Surgewave.Broker.Native.NativeGroupCoordinator(nativeGroupCoordinatorLogger, _offsetStore);
        var producerStateManager = new ProducerStateManager();
        var transactionIndex = new TransactionIndex();
        _transactionStateStore = new TransactionStateStore(_dataDirectory!, txnStateStoreLogger);
        var transactionCoordinator = new TransactionCoordinator(
            producerStateManager, _logManager, transactionIndex, _offsetStore, _transactionStateStore, txnCoordinatorLogger);
        var quotaManagerLogger = _loggerFactory.CreateLogger<QuotaManager>();
        _quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger);
        IProtocolHandler protocolHandler = new KafkaProtocolHandler();

        // The point of this fixture: wire OAuthBearerAuthenticator into the SASL stack.
        // The HttpClient is held by the validator for JWKS refresh, so it must outlive
        // the broker — keep it as a field disposed by DisposeAsync.
        _oauthHttp = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var validator = new JwksTokenValidator(config.Security.OAuthBearer, jwksLogger, _oauthHttp);
        var oauthBearerAuthenticator = new OAuthBearerAuthenticator(validator, config.Security.OAuthBearer);
        var saslAuthenticator = new SaslAuthenticator(
            new CredentialStore(),
            config.Security.SaslMechanisms,
            oauthBearer: oauthBearerAuthenticator);

        var dataApiHandler = new DataApiHandler(
            config, _logManager, transactionCoordinator, _quotaManager, recordBatchSerializer, null, null, null, null, null,
            _loggerFactory.CreateLogger<DataApiHandler>());
        var metadataApiHandler = new MetadataApiHandler(
            config, _logManager, _loggerFactory.CreateLogger<MetadataApiHandler>());
        var topicAdminHandler = new TopicAdminHandler(
            config, _logManager, _quotaManager, auditLogger: null, _loggerFactory.CreateLogger<TopicAdminHandler>());
        var dynamicBrokerConfig = new DynamicBrokerConfig(config, _loggerFactory.CreateLogger<DynamicBrokerConfig>());
        var configApiHandler = new ConfigApiHandler(config, dynamicBrokerConfig, _logManager);
        var securityApiHandler = new SecurityApiHandler(
            config, saslAuthenticator, aclAuthorizer: null, auditLogger: null, _loggerFactory.CreateLogger<SecurityApiHandler>());

        var handlers = new IKafkaRequestHandler[]
        {
            dataApiHandler, metadataApiHandler, topicAdminHandler, configApiHandler, securityApiHandler
        };
        var dispatcher = new RequestDispatcher(handlers);

        _metrics = new BrokerMetrics();
        _broker = new SurgewaveBroker(
            config, _logManager, recordBatchSerializer, consumerGroupCoordinator, shareGroupCoordinator, nativeGroupCoordinator,
            transactionCoordinator, _quotaManager, protocolHandler, _metrics, dispatcher, brokerLogger);

        _cts = new CancellationTokenSource();
        _brokerTask = Task.Run(() => _broker.StartAsync(_cts.Token));
        await Task.Yield();
    }

    private static int FindAvailablePort()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.IPv6Any, 0);
        listener.Server.DualMode = true;
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task WaitForBrokerReady(int port, int maxRetries = 30)
    {
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                await client.ConnectAsync("localhost", port);
                return;
            }
            catch { await Task.Delay(100); }
        }
        throw new InvalidOperationException($"OAUTHBEARER broker did not start within {maxRetries * 100}ms");
    }

    public async ValueTask DisposeAsync()
    {
        if (_instance == this) _instance = null;

        var cts = Interlocked.Exchange(ref _cts, null);
        if (cts != null) { await cts.CancelAsync(); cts.Dispose(); }

        var broker = Interlocked.Exchange(ref _broker, null);
        if (broker != null) await broker.DisposeAsync();

        var jwksCts = Interlocked.Exchange(ref _jwksCts, null);
        jwksCts?.Cancel();
        _jwksListener?.Stop();
        _jwksListener?.Close();
        if (_jwksTask != null)
        {
            try { await _jwksTask; } catch { /* shutting down */ }
        }
        jwksCts?.Dispose();

        _signingKey?.Dispose();
        _oauthHttp?.Dispose();
        _metrics?.Dispose();
        _quotaManager?.Dispose();
        _transactionStateStore?.Dispose();
        _offsetStore?.Dispose();
        _logManager?.Dispose();
        _loggerFactory.Dispose();

        try
        {
            if (_dataDirectory != null && Directory.Exists(_dataDirectory))
                Directory.Delete(_dataDirectory, recursive: true);
        }
        catch { /* best effort */ }
    }

    public void Dispose() => DisposeAsync().GetAwaiter().GetResult();
}

[CollectionDefinition("OAuthBearerBroker")]
public class OAuthBearerBrokerCollection : ICollectionFixture<OAuthBearerBrokerFixture>
{
}
