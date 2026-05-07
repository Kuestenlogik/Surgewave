using Kuestenlogik.Surgewave.Marketplace;
using Kuestenlogik.Surgewave.Marketplace.Services;
using Kuestenlogik.Surgewave.Marketplace.Storage;
using Microsoft.Extensions.Options;
using MudBlazor.Services;

var builder = WebApplication.CreateBuilder(args);

var dataDir = builder.Configuration["Surgewave:Marketplace:DataDirectory"] ?? "./data";
Directory.CreateDirectory(dataDir);

// Marketplace signature-enforcement options. Optional by default: unsigned uploads pass
// through unless Surgewave:Marketplace:Signing:RequireSignedUploads=true.
builder.Services.Configure<MarketplaceSignerOptions>(
    builder.Configuration.GetSection(MarketplaceSignerOptions.ConfigSection));

// Storage + Metadata
var storage = new FileSystemPackageStorage(dataDir);
var metadata = new FileSystemMetadataService(dataDir);
builder.Services.AddSingleton<IPackageStorageService>(storage);
builder.Services.AddSingleton<IPackageMetadataService>(metadata);

// Blazor + MudBlazor
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();
builder.Services.AddMudServices();

// Enable static web assets in development
if (builder.Environment.IsDevelopment())
{
    builder.WebHost.UseStaticWebAssets();
}

var app = builder.Build();

// Initialize metadata index
await metadata.InitializeAsync();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseStaticFiles();
app.UseAntiforgery();

// REST API
var signerOptions = app.Services.GetRequiredService<IOptions<MarketplaceSignerOptions>>().Value;
app.MapMarketplaceApi(storage, metadata, signerOptions);

// Health check
app.MapGet("/health", () => Results.Ok(new { service = "Surgewave Marketplace", status = "running" }));

// Blazor UI
app.MapRazorComponents<Kuestenlogik.Surgewave.Marketplace.Components.App>()
    .AddInteractiveServerRenderMode();

app.Run();
