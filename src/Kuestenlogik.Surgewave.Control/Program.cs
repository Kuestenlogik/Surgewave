using Kuestenlogik.Surgewave.Control.Components;
using Kuestenlogik.Surgewave.Control.Hubs;
using Kuestenlogik.Surgewave.Control.Models.Assistant;
using Kuestenlogik.Surgewave.Control.Security;
using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Control.Models.Marketplace;
using Kuestenlogik.Surgewave.Control.Services.Assistant;
using Kuestenlogik.Surgewave.Control.Services.Collaboration;
using Kuestenlogik.Surgewave.Control.Services.Timeline;
using Kuestenlogik.Surgewave.Control.State;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using MudBlazor.Services;
using Blazored.LocalStorage;

var builder = WebApplication.CreateBuilder(args);

// Enable static web assets in development (for NuGet packages like MudBlazor, Z.Blazor.Diagrams)
// In production, assets are included via dotnet publish
if (builder.Environment.IsDevelopment())
{
    builder.WebHost.UseStaticWebAssets();
}

// Bind auth configuration
var authConfig = new SurgewaveAuthConfig();
builder.Configuration.GetSection(SurgewaveAuthConfig.SectionName).Bind(authConfig);
builder.Services.Configure<SurgewaveAuthConfig>(builder.Configuration.GetSection(SurgewaveAuthConfig.SectionName));

// Configure authentication (only when enabled)
if (authConfig.Enabled && authConfig.Providers.Length > 0)
{
    var authBuilder = builder.Services.AddAuthentication(options =>
    {
        options.DefaultScheme = CookieAuthenticationDefaults.AuthenticationScheme;

        if (authConfig.IsSingleProvider)
        {
            var single = authConfig.Providers[0];
            // Single provider: direct challenge to the named OIDC scheme (SAML/LDAP use cookie challenge)
            options.DefaultChallengeScheme = single.Type is AuthProviderType.Saml or AuthProviderType.Ldap
                ? CookieAuthenticationDefaults.AuthenticationScheme
                : SchemeNames.OidcScheme(single.Name);
        }
        else
        {
            // Multi provider: cookie challenge → login page with provider selection
            options.DefaultChallengeScheme = CookieAuthenticationDefaults.AuthenticationScheme;
        }
    });

    authBuilder.AddSurgewaveProviders(authConfig, builder.Services);

    if (authConfig.HasSamlProvider || authConfig.HasLdapProvider)
    {
        builder.Services.AddControllers();
    }

    // Register all claims transformers as concrete types, composite as IClaimsTransformation
    builder.Services.AddTransient<KeycloakClaimsTransformation>();
    builder.Services.AddTransient<EntraIdClaimsTransformation>();
    builder.Services.AddTransient<SamlClaimsTransformation>();
    builder.Services.AddTransient<OktaClaimsTransformation>();
    builder.Services.AddTransient<GoogleClaimsTransformation>();
    builder.Services.AddTransient<LdapClaimsTransformation>();
    builder.Services.AddTransient<IClaimsTransformation, CompositeClaimsTransformation>();

    builder.Services.AddAuthorizationBuilder()
        .AddSurgewavePolicies(authConfig);

    builder.Services.AddHttpContextAccessor();
    builder.Services.AddTransient<BearerTokenDelegatingHandler>();
    builder.Services.AddCascadingAuthenticationState();
}

// Add MudBlazor services
builder.Services.AddMudServices(config =>
{
    config.SnackbarConfiguration.PositionClass = MudBlazor.Defaults.Classes.Position.BottomRight;
    config.SnackbarConfiguration.PreventDuplicates = false;
    config.SnackbarConfiguration.NewestOnTop = true;
    config.SnackbarConfiguration.ShowCloseIcon = true;
    config.SnackbarConfiguration.VisibleStateDuration = 5000;
});

// Add Blazored LocalStorage
builder.Services.AddBlazoredLocalStorage();

// Add Razor components with interactive server-side rendering
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Configure HTTP client for Gateway/Broker API.
// Default matches the broker's default Kestrel binding (https on :9093). Override via
// Broker:ApiUrl in appsettings when the broker runs with Surgewave:GrpcUseTls=false.
var brokerApiUrl = builder.Configuration["Broker:ApiUrl"] ?? "https://localhost:9093";

void ConfigureHttpClient(HttpClient client, int timeoutSeconds = 30)
{
    client.BaseAddress = new Uri(brokerApiUrl);
    client.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
}

IHttpClientBuilder AddBearerTokenHandler(IHttpClientBuilder clientBuilder)
{
    if (authConfig.Enabled)
        clientBuilder.AddHttpMessageHandler<BearerTokenDelegatingHandler>();
    return clientBuilder;
}

AddBearerTokenHandler(builder.Services.AddHttpClient<ISurgewaveApiClient, SurgewaveApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IPipelineApiClient, PipelineApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IConnectorRegistryService, ConnectorRegistryService>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IAclApiClient, AclApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IQuotaApiClient, QuotaApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IBandwidthQuotaApiClient, BandwidthQuotaApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<ISchemaRegistryClient, SchemaRegistryClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IAuditApiClient, AuditApiClient>(c => ConfigureHttpClient(c)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IMetricsClient, MetricsClient>(c => ConfigureHttpClient(c, 10)));
AddBearerTokenHandler(builder.Services.AddHttpClient<IChatApiClient, ChatApiClient>(c => ConfigureHttpClient(c, 120)));

// Register application state
builder.Services.AddScoped<ClusterState>();
builder.Services.AddScoped<NotificationState>();
builder.Services.AddScoped<ShepherdService>();

// Register Marketplace service (singleton because it manages repository connections)
builder.Services.AddSingleton<IConnectorMarketplaceService, ConnectorMarketplaceService>();

// Register Marketplace config and Review service (scoped per Blazor circuit, uses browser LocalStorage)
var marketplaceConfig = builder.Configuration.GetSection("Surgewave:Marketplace").Get<MarketplaceConfig>() ?? new MarketplaceConfig();
builder.Services.AddSingleton(marketplaceConfig);
builder.Services.AddScoped<IReviewService, ReviewService>();

// Register Agent config service
builder.Services.AddScoped<IAgentConfigService, AgentConfigService>();

// Register Pipeline advanced services
builder.Services.AddScoped<SchemaPreviewService>();
builder.Services.AddSingleton<PipelineLineageService>();

// Register Assistant services (scoped per Blazor circuit)
builder.Services.AddScoped<AssistantState>();
builder.Services.AddScoped<AssistantSettings>();
builder.Services.AddScoped<IMetricsAnalyzer, MetricsAnalyzer>();
builder.Services.AddScoped<ITuningAdvisor, TuningAdvisor>();
builder.Services.AddHttpClient<ILlmClient, LlmClient>((sp, client) =>
{
    var settings = sp.GetRequiredService<AssistantSettings>();
    if (!string.IsNullOrWhiteSpace(settings.LlmEndpoint))
    {
        client.BaseAddress = new Uri(settings.LlmEndpoint);
    }
    client.Timeout = TimeSpan.FromSeconds(60);
});
builder.Services.AddScoped<INlToSqlTranslator, NlToSqlTranslator>();
builder.Services.AddScoped<IAssistantService, AssistantService>();

// Register Timeline Debugger service
builder.Services.AddScoped<ITimelineService, TimelineService>();

// Register Collaboration services (SignalR for multi-user pipeline editing)
builder.Services.AddSignalR();
builder.Services.AddSingleton<CollaborationStateService>();

// Control UI plugin discovery (Fleet, Schema Registry, etc.)
var controlPluginRegistry = new ControlPluginRegistry();
using (var lf = LoggerFactory.Create(b => b.AddConsole()))
{
    controlPluginRegistry.DiscoverPlugins("plugins", lf.CreateLogger<ControlPluginRegistry>());
}
builder.Services.AddSingleton(controlPluginRegistry);

var app = builder.Build();

// Configure the HTTP request pipeline
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

if (authConfig.Enabled)
{
    app.UseAuthentication();
    app.UseAuthorization();
}

app.UseAntiforgery();

if (authConfig is { Enabled: true } && (authConfig.HasSamlProvider || authConfig.HasLdapProvider))
{
    app.MapControllers();
}

app.MapStaticAssets();

// Map SignalR hub for collaborative pipeline editing
app.MapHub<PipelineCollaborationHub>("/hubs/collaboration");

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
