using System.Net.Http;
using System.Security.Cryptography;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Read;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Coordination.Streams;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Protocol plugin advertising the Kafka wire protocol — native-first Surgewave's optional
/// compatibility layer (#58 / #41). Kafka rides the broker's shared TCP listener (auto-detected
/// by the first magic bytes), so this plugin opens no port of its own.
/// <para>
/// #59 b5 ATOMIC FLIP: the entire Kafka wire stack now lives here. <see cref="ConfigureServices"/>
/// registers the request handlers, the SASL/SCRAM/OAUTHBEARER stack, the inter-broker
/// ControllerClient/BrokerLifecycle/TxnMarker wire codecs, and the <see cref="KafkaConnectionHandler"/>
/// that owns the connection loop — all resolving the broker's protocol-neutral service seams
/// (IBrokerConfigView / IQuotaManager / IAuthorizer / IBrokerMetrics / the coordinator interfaces /
/// the clustering types). The broker host no longer carries any Kafka-named type.
/// </para>
/// </summary>
public sealed class SurgewaveKafkaProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.Kafka";
    public string DisplayName => "Kafka Protocol";

    /// <summary>0 — Kafka shares the broker's main listener; there is no separate Kafka port.</summary>
    public int DefaultPort => 0;

    /// <summary>
    /// Enabled by default (unlike opt-in protocols): Kafka compatibility is on unless the operator
    /// sets <c>Surgewave:Kafka:Enabled=false</c> to run native-only.
    /// </summary>
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Kafka:Enabled", true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // The broker's static config type (BrokerConfig) is not visible here; surface the neutral
        // scalars the handlers need from IConfiguration / IBrokerConfigView instead.
        var pipelineDepth = configuration.GetValue<int>("Surgewave:KafkaPipelineDepth", 16);

        // The Telemetry handler wants ClientTelemetryConfig (Broker.Abstractions); expose it as a
        // DI service resolved off the neutral config view is not possible (not on the view), so bind
        // it from configuration directly.
        services.AddSingleton(sp =>
            configuration.GetSection("Surgewave:Telemetry").Get<ClientTelemetryConfig>() ?? new ClientTelemetryConfig());

        // ── SASL / SCRAM / OAUTHBEARER stack ─────────────────────────────────────
        RegisterSasl(services, configuration);

        // ── Request handlers (IKafkaRequestHandler) ──────────────────────────────
        services.AddSingleton<IKafkaRequestHandler>(sp => new DataApiHandler(
            sp.GetRequiredService<IBrokerConfigView>(),
            sp.GetRequiredService<LogManager>(),
            sp.GetRequiredService<IProduceTransactionCoordinator>(),
            sp.GetRequiredService<IQuotaManager>(),
            sp.GetRequiredService<RecordBatchSerializer>(),
            sp.GetService<IAuthorizer>(),
            sp.GetService<IDeduplicationManager>(),
            sp.GetService<IDelayIndex>(),
            sp.GetService<ITtlIndex>(),
            sp.GetService<IBrokerMetrics>(),
            sp.GetRequiredService<ILogger<DataApiHandler>>(),
            sp.GetService<IBandwidthQuota>(),
            sp.GetService<SurgewaveBrokerObservability>(),
            coldStartProfiler: sp.GetService<IColdStartProfiler>(),
            partitionAppender: sp.GetService<IPartitionAppender>(),
            disaggregatedReader: sp.GetService<IDisaggregatedSegmentReader>()));

        services.AddSingleton<IKafkaRequestHandler>(sp =>
        {
            var metadataApiHandler = new MetadataApiHandler(
                sp.GetRequiredService<IBrokerConfigView>(),
                sp.GetRequiredService<LogManager>(),
                sp.GetRequiredService<ILogger<MetadataApiHandler>>());
            metadataApiHandler.SetClusterState(sp.GetRequiredService<ClusterState>());
            metadataApiHandler.SetClusterTopicCreator(sp.GetRequiredService<ClusterController>());
            return metadataApiHandler;
        });

        services.AddSingleton<IKafkaRequestHandler>(sp => new TopicAdminHandler(
            sp.GetRequiredService<IBrokerConfigView>(),
            sp.GetRequiredService<LogManager>(),
            sp.GetRequiredService<IQuotaManager>(),
            sp.GetService<IAuditLogger>(),
            sp.GetRequiredService<ILogger<TopicAdminHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ConfigApiHandler(
            sp.GetRequiredService<IBrokerConfigView>(),
            sp.GetRequiredService<IDynamicBrokerConfig>(),
            sp.GetRequiredService<LogManager>()));

        services.AddSingleton<IKafkaRequestHandler>(sp =>
        {
            var scram = sp.GetRequiredService<ScramStores>();
            return new SecurityApiHandler(
                sp.GetRequiredService<IBrokerConfigView>(),
                sp.GetService<SaslAuthenticator>(),
                sp.GetService<IAuthorizer>(),
                sp.GetService<IAuditLogger>(),
                sp.GetRequiredService<ILogger<SecurityApiHandler>>(),
                scramSha256Store: scram.Sha256,
                scramSha512Store: scram.Sha512);
        });

        services.AddSingleton<IKafkaRequestHandler>(sp => new InterBrokerApiHandler(
            sp.GetRequiredService<IBrokerConfigView>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<ReplicaManager>(),
            sp.GetRequiredService<LogManager>(),
            sp.GetRequiredService<ILogger<InterBrokerApiHandler>>(),
            sp.GetService<ITransactionMarkerSink>(),
            isrUpdateApplier: sp.GetRequiredService<ClusterController>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ConsumerGroupApiHandler(
            sp.GetRequiredService<IConsumerGroupCoordinator>(),
            sp.GetRequiredService<ILogger<ConsumerGroupApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ShareGroupApiHandler(
            sp.GetRequiredService<IShareGroupCoordinator>(),
            sp.GetRequiredService<ILogger<ShareGroupApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new TransactionApiHandler(
            sp.GetRequiredService<ITransactionCoordinator>(),
            sp.GetRequiredService<ILogger<TransactionApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ConsumerGroupV2ApiHandler(
            sp.GetRequiredService<IConsumerGroupV2Coordinator>(),
            sp.GetRequiredService<ILogger<ConsumerGroupV2ApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new StreamsGroupApiHandler(
            sp.GetRequiredService<IStreamsGroupCoordinator>(),
            sp.GetRequiredService<ILogger<StreamsGroupApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ClusterAdminHandler(
            sp.GetRequiredService<ClusterController>(),
            sp.GetRequiredService<PartitionReassignmentManager>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<ILogger<ClusterAdminHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new QuotaApiHandler(
            sp.GetRequiredService<IQuotaManager>(),
            sp.GetRequiredService<ILogger<QuotaApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new DelegationTokenApiHandler(
            sp.GetRequiredService<IDelegationTokenService>(),
            sp.GetRequiredService<ILogger<DelegationTokenApiHandler>>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new TelemetryApiHandler(
            sp.GetRequiredService<ILogger<TelemetryApiHandler>>(),
            sp.GetRequiredService<ClientTelemetryConfig>(),
            sp.GetRequiredService<ITelemetryIngestor>()));

        services.AddSingleton<IKafkaRequestHandler>(sp => new ClusterMembershipHandler(
            sp.GetRequiredService<ClusterIdManager>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<ILogger<ClusterMembershipHandler>>()));

        // RaftApiHandler: RaftNode / RaftPersistence are optional (registered only under
        // UseRaftConsensus). Resolve them nullably so the handler answers NotController otherwise.
        services.AddSingleton<IKafkaRequestHandler>(sp => new RaftApiHandler(
            sp.GetRequiredService<IBrokerConfigView>(),
            sp.GetService<RaftNode>(),
            sp.GetService<RaftPersistence>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<ILogger<RaftApiHandler>>()));

        // ── Inter-broker wire codecs (relocated with the plugin, #59 b5) ─────────
        services.AddSingleton<IControllerReplicaRpc>(sp => new ControllerClient(
            sp.GetRequiredService<ConnectionPool>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<ClusteringConfig>(),
            sp.GetRequiredService<ILogger<ControllerClient>>()));

        services.AddSingleton<ITransactionMarkerReplicator>(sp => new TransactionMarkerReplicator(
            sp.GetRequiredService<ConnectionPool>(),
            sp.GetRequiredService<ClusterState>(),
            sp.GetRequiredService<IBrokerConfigView>().BrokerId,
            sp.GetRequiredService<ILogger<TransactionMarkerReplicator>>()));

        // ── The connection handler that owns the Kafka wire loop ─────────────────
        services.AddSingleton<IConnectionHandler>(sp => new KafkaConnectionHandler(
            sp.GetServices<IKafkaRequestHandler>(),
            pipelineDepth,
            sp.GetService<IBrokerMetrics>(),
            sp.GetRequiredService<ILogger<KafkaConnectionHandler>>(),
            sp.GetService<ILogger<RequestDispatcher>>()));
    }

    /// <summary>
    /// Registers the Kafka SASL stack (SCRAM stores + <see cref="SaslAuthenticator"/>). The concrete
    /// stores/authenticator moved into the plugin (#59 b5); config is read from the neutral view and
    /// IConfiguration since the broker's SecurityConfig type is not visible here.
    /// </summary>
    private static void RegisterSasl(IServiceCollection services, IConfiguration configuration)
    {
        // SCRAM stores holder — always registered; both entries null unless SASL is on and the
        // mechanism is in the allow-list. Registered even when SASL is off so SecurityApiHandler
        // can resolve it unconditionally (it gets nulls).
        services.AddSingleton(sp =>
        {
            var view = sp.GetRequiredService<IBrokerConfigView>();
            if (!view.SaslEnabled)
                return new ScramStores(null, null);

            var logger = sp.GetRequiredService<ILogger<SaslAuthenticator>>();
            var mechanisms = view.SaslMechanisms;
            ScramCredentialStore? sha256 = null;
            ScramCredentialStore? sha512 = null;
            if (mechanisms.Contains(SaslAuthenticator.MechanismScramSha256, StringComparer.OrdinalIgnoreCase))
            {
                sha256 = new ScramCredentialStore(hashAlgorithm: HashAlgorithmName.SHA256);
                logger.LogInformation("SCRAM-SHA-256 store initialised (in-memory)");
            }
            if (mechanisms.Contains(SaslAuthenticator.MechanismScramSha512, StringComparer.OrdinalIgnoreCase))
            {
                sha512 = new ScramCredentialStore(hashAlgorithm: HashAlgorithmName.SHA512);
                logger.LogInformation("SCRAM-SHA-512 store initialised (in-memory)");
            }
            return new ScramStores(sha256, sha512);
        });

        var saslEnabled = configuration.GetValue<bool>("Surgewave:Security:SaslEnabled", false);
        if (saslEnabled)
            services.AddSingleton(sp => BuildSaslAuthenticator(sp, configuration));
    }

    private static SaslAuthenticator BuildSaslAuthenticator(IServiceProvider sp, IConfiguration configuration)
    {
        var view = sp.GetRequiredService<IBrokerConfigView>();
        var logger = sp.GetRequiredService<ILogger<SaslAuthenticator>>();

        var mechanisms = view.SaslMechanisms;
        var credentialsFile = configuration.GetValue<string?>("Surgewave:Security:CredentialsFile");
        var users = configuration.GetSection("Surgewave:Security:Users").Get<string[]>() ?? [];

        var credentialStore = new CredentialStore(credentialsFile);
        foreach (var userEntry in users)
        {
            var parts = userEntry.Split(':');
            if (parts.Length == 2)
                credentialStore.AddUser(parts[0], parts[1]);
        }

        // OAUTHBEARER (KIP-936): stand up a JWT validator + frame parser only when the mechanism is
        // in the allow-list and the OIDC/JWKS config is present; otherwise the mechanism is rejected.
        OAuthBearerAuthenticator? oauthBearer = null;
        var oauth = configuration.GetSection("Surgewave:Security:OAuthBearer").Get<OAuthBearerConfig>() ?? new OAuthBearerConfig();
        if (oauth.Enabled
            && mechanisms.Contains(SaslAuthenticator.MechanismOAuthBearer, StringComparer.OrdinalIgnoreCase))
        {
            var oauthHttp = sp.GetRequiredService<IHttpClientFactory>().CreateClient("oauthbearer-jwks");
            oauthHttp.Timeout = TimeSpan.FromSeconds(30);
            var validator = new JwksTokenValidator(oauth, sp.GetRequiredService<ILogger<JwksTokenValidator>>(), oauthHttp);
            oauthBearer = new OAuthBearerAuthenticator(validator, oauth);
            logger.LogInformation(
                "OAUTHBEARER enabled (issuer={Issuer}, jwksUri={Jwks}, principalClaim={Claim})",
                oauth.ValidIssuer ?? oauth.OidcAuthority ?? "(none)",
                oauth.JwksUri ?? "(via discovery)",
                oauth.PrincipalClaim);
        }

        var scram = sp.GetRequiredService<ScramStores>();
        var authenticator = new SaslAuthenticator(
            credentialStore,
            mechanisms,
            scramSha256Store: scram.Sha256,
            scramSha512Store: scram.Sha512,
            oauthBearer: oauthBearer);
        logger.LogInformation("SASL authentication enabled with mechanisms: {Mechanisms}",
            string.Join(", ", mechanisms));
        return authenticator;
    }
}

/// <summary>
/// Holds the two optional in-memory SCRAM credential stores as a single DI service so the
/// <see cref="SaslAuthenticator"/> and the SecurityApiHandler (AlterUserScramCredentials, KIP-554)
/// share the SAME instances. Either entry is null when the mechanism is not in the allow-list.
/// </summary>
internal sealed record ScramStores(ScramCredentialStore? Sha256, ScramCredentialStore? Sha512);
