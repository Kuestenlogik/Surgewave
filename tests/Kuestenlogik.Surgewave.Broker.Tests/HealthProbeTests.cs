using Kuestenlogik.Surgewave.Broker.Health;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The container image holds no HTTP client, so a container health check asks through the broker executable itself
/// (<c>surgewave-broker health</c>). The probe takes the endpoint from the configuration the broker starts with: a
/// health check that names the port a second time is wrong the day the port changes.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class HealthProbeTests
{
    [Theory]
    [InlineData("http://*:9093", "http://localhost:9093/")]
    [InlineData("http://+:9095", "http://localhost:9095/")]
    [InlineData("http://0.0.0.0:9095", "http://localhost:9095/")]
    [InlineData("http://[::]:9095", "http://localhost:9095/")]
    [InlineData("https://*:9443", "https://localhost:9443/")]
    [InlineData("http://10.1.2.3:9095", "http://10.1.2.3:9095/")]
    public void AsksTheEndpointTheBrokerBinds_OnLoopbackWhereThatIsEveryInterface(string kestrelUrl, string expected)
    {
        var configuration = Configuration(("Kestrel:Endpoints:Grpc:Url", kestrelUrl));

        Assert.Equal(expected, HealthProbe.Endpoint(configuration).ToString());
    }

    [Fact]
    public void WithGrpcUseTls_AsksOverHttpsOnTheGrpcPort_AsTheBrokerBindsIt()
    {
        var configuration = Configuration(
            ("Kestrel:Endpoints:Grpc:Url", "http://*:9093"),
            ("Surgewave:GrpcUseTls", "true"),
            ("Surgewave:GrpcPort", "9443"));

        Assert.Equal("https://localhost:9443/", HealthProbe.Endpoint(configuration).ToString());
    }

    [Fact]
    public void WithoutGrpcUseTls_AsksOverHttp_WhateverSchemeTheEndpointIsWrittenWith()
    {
        var configuration = Configuration(
            ("Kestrel:Endpoints:Grpc:Url", "https://*:9093"),
            ("Surgewave:GrpcUseTls", "false"));

        Assert.Equal("http://localhost:9093/", HealthProbe.Endpoint(configuration).ToString());
    }

    [Fact]
    public void WithoutAnEndpoint_AsksTheGrpcPort()
    {
        var configuration = Configuration(("Surgewave:GrpcPort", "9555"));

        Assert.Equal("http://localhost:9555/", HealthProbe.Endpoint(configuration).ToString());
    }

    [Fact]
    public async Task AnUnknownCheck_IsRefusedWithTheOnesThereAre()
    {
        using var output = new StringWriter();

        var exitCode = await HealthProbe.RunAsync(["started"], output, TestContext.Current.CancellationToken);

        Assert.Equal(2, exitCode);
        Assert.Contains("'ready' or 'live'", output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABrokerThatDoesNotAnswer_IsUnhealthy()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        using var output = new StringWriter();

        var exitCode = await HealthProbe.RunAsync(
            ["ready", $"--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:{port}", "--Surgewave:GrpcUseTls=false"],
            output, TestContext.Current.CancellationToken);

        Assert.Equal(1, exitCode);
    }

    private static IConfiguration Configuration(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();
}
