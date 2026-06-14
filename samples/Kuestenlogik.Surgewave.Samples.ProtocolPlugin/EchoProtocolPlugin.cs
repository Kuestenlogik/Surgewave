using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Samples.ProtocolPlugin;

/// <summary>
/// Reference <see cref="IProtocolPlugin"/>. Returns <c>0</c> from
/// <see cref="DefaultPort"/> to signal it shares the broker's HTTP
/// host instead of opening its own TCP listener — the broker then
/// maps the plugin's endpoints onto its existing Kestrel pipeline.
///
/// A real protocol adapter (MQTT, AMQP, …) would return a non-zero
/// default port, register a hosted service that opens a TCP listener
/// in <see cref="ConfigureServices"/>, and wire the
/// per-connection state machine that decodes the wire format.
/// This sample stays at the contract surface so the example fits
/// into one file.
/// </summary>
public sealed class EchoProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Kuestenlogik.Surgewave.Samples.ProtocolPlugin";
    public string DisplayName => "Sample Protocol Plugin";

    /// <summary>0 = share the broker's HTTP port (no separate TCP listener).</summary>
    public int DefaultPort => 0;

    public bool IsConfigEnabled(IConfiguration configuration) =>
        configuration.GetValue<bool>("SampleProtocolPlugin:Enabled", defaultValue: true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // No services registered — the endpoint mapping happens in Configure.
    }

    public void Configure(object host, IServiceProvider services)
    {
        // The host is a Microsoft.AspNetCore.Builder.WebApplication; a real
        // plugin would cast it and map endpoints, e.g.:
        //
        //   var app = (WebApplication)host;
        //   app.MapPost("/echo", async ctx =>
        //   {
        //       var body = await new StreamReader(ctx.Request.Body).ReadToEndAsync();
        //       await ctx.Response.WriteAsync(body);
        //   });
        //
        // We avoid the actual cast in the sample so the project does not
        // need to pull in Microsoft.AspNetCore.App framework reference —
        // operators see the cast in the comment + can copy it into their
        // own project verbatim.
    }
}
