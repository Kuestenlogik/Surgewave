using Kuestenlogik.Surgewave.Broker.Hosting;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// <c>Surgewave:GrpcUseTls</c> is the one switch for the broker's two HTTP endpoints (#276): <c>false</c> alone yields
/// cleartext and <c>true</c> HTTPS, whatever scheme the endpoint URLs are written with, so that an image, a manifest
/// or a test changes the setting and not the endpoints. The configuration here is the broker's own appsettings.json.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BrokerEndpointsTests
{
    private static readonly (string Key, string Value)[] Shipped =
    [
        ("Kestrel:Endpoints:Grpc:Url", "https://*:9093"),
        ("Kestrel:Endpoints:Grpc:Protocols", "Http1AndHttp2"),
        ("Kestrel:Endpoints:GrpcHttp3:Url", "https://*:9094"),
        ("Kestrel:Endpoints:GrpcHttp3:Protocols", "Http1AndHttp2AndHttp3"),
        ("Surgewave:GrpcPort", "9093"),
    ];

    [Fact]
    public void WithoutTls_RestIsServedOverHttp1OnTheGrpcPort_AndGrpcOverHttp2OnTheSecondEndpoint()
    {
        var configuration = Configuration([.. Shipped, ("Surgewave:GrpcUseTls", "false")]);

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("http://*:9093", configuration["Kestrel:Endpoints:Grpc:Url"]);
        Assert.Equal("Http1", configuration["Kestrel:Endpoints:Grpc:Protocols"]);
        Assert.Equal("http://*:9094", configuration["Kestrel:Endpoints:GrpcHttp3:Url"]);
        Assert.Equal("Http2", configuration["Kestrel:Endpoints:GrpcHttp3:Protocols"]);
        Assert.Equal(9094, BrokerEndpoints.CleartextGrpcPort(configuration));
    }

    [Fact]
    public void WithoutTls_AnEndpointKeepsItsHostAndPort()
    {
        var configuration = Configuration(
            ("Surgewave:GrpcUseTls", "false"),
            ("Kestrel:Endpoints:Grpc:Url", "https://127.0.0.1:19093"),
            ("Kestrel:Endpoints:GrpcHttp3:Url", "http://127.0.0.1:19094"));

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("http://127.0.0.1:19093", configuration["Kestrel:Endpoints:Grpc:Url"]);
        Assert.Equal("http://127.0.0.1:19094", configuration["Kestrel:Endpoints:GrpcHttp3:Url"]);
        Assert.Equal(19094, BrokerEndpoints.CleartextGrpcPort(configuration));
    }

    [Fact]
    public void WithoutTls_ProtocolsThatWorkWithoutItAreKept()
    {
        var configuration = Configuration(
            ("Surgewave:GrpcUseTls", "false"),
            ("Kestrel:Endpoints:Grpc:Url", "http://*:9093"),
            ("Kestrel:Endpoints:Grpc:Protocols", "Http2"),
            ("Kestrel:Endpoints:GrpcHttp3:Url", "http://*:9094"),
            ("Kestrel:Endpoints:GrpcHttp3:Protocols", "Http1"));

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("Http2", configuration["Kestrel:Endpoints:Grpc:Protocols"]);
        Assert.Equal("Http1", configuration["Kestrel:Endpoints:GrpcHttp3:Protocols"]);
        Assert.Equal(9093, BrokerEndpoints.CleartextGrpcPort(configuration));
    }

    [Fact]
    public void WithoutTls_AndWithoutASecondEndpoint_NoneIsAdded_AndGrpcIsNotServed()
    {
        var configuration = Configuration(("Surgewave:GrpcUseTls", "false"), ("Surgewave:GrpcPort", "9555"));

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("http://*:9555", configuration["Kestrel:Endpoints:Grpc:Url"]);
        Assert.Null(configuration["Kestrel:Endpoints:GrpcHttp3:Url"]);
        Assert.Null(BrokerEndpoints.CleartextGrpcPort(configuration));
    }

    [Fact]
    public void WithTls_BothEndpointsAreHttps_WithTheirProtocolsAsWritten()
    {
        var configuration = Configuration(
            ("Surgewave:GrpcUseTls", "true"),
            ("Surgewave:GrpcPort", "9443"),
            ("Kestrel:Endpoints:Grpc:Url", "http://*:9093"),
            ("Kestrel:Endpoints:Grpc:Protocols", "Http1AndHttp2"),
            ("Kestrel:Endpoints:GrpcHttp3:Url", "http://*:9094"),
            ("Kestrel:Endpoints:GrpcHttp3:Protocols", "Http1AndHttp2AndHttp3"));

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("https://*:9443", configuration["Kestrel:Endpoints:Grpc:Url"]);
        Assert.Equal("Http1AndHttp2", configuration["Kestrel:Endpoints:Grpc:Protocols"]);
        Assert.Equal("https://*:9094", configuration["Kestrel:Endpoints:GrpcHttp3:Url"]);
        Assert.Equal("Http1AndHttp2AndHttp3", configuration["Kestrel:Endpoints:GrpcHttp3:Protocols"]);
        Assert.Null(BrokerEndpoints.CleartextGrpcPort(configuration));
    }

    [Fact]
    public void WithTls_TheCertificateIsGivenToBothEndpoints()
    {
        var configuration = Configuration(
        [
            .. Shipped,
            ("Surgewave:GrpcUseTls", "true"),
            ("Surgewave:GrpcCertificatePath", "/certs/broker.pfx"),
            ("Surgewave:GrpcCertificatePassword", "changeit"),
        ]);

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("/certs/broker.pfx", configuration["Kestrel:Endpoints:Grpc:Certificate:Path"]);
        Assert.Equal("changeit", configuration["Kestrel:Endpoints:Grpc:Certificate:Password"]);
        Assert.Equal("/certs/broker.pfx", configuration["Kestrel:Endpoints:GrpcHttp3:Certificate:Path"]);
        Assert.Equal("changeit", configuration["Kestrel:Endpoints:GrpcHttp3:Certificate:Password"]);
    }

    [Fact]
    public void WithTls_ASecondEndpointWithACertificateOfItsOwnKeepsIt()
    {
        var configuration = Configuration(
        [
            .. Shipped,
            ("Surgewave:GrpcUseTls", "true"),
            ("Surgewave:GrpcCertificatePath", "/certs/broker.pfx"),
            ("Kestrel:Endpoints:GrpcHttp3:Certificate:Path", "/certs/http3.pem"),
            ("Kestrel:Endpoints:GrpcHttp3:Certificate:KeyPath", "/certs/http3.key"),
        ]);

        BrokerEndpoints.Apply(configuration);

        Assert.Equal("/certs/http3.pem", configuration["Kestrel:Endpoints:GrpcHttp3:Certificate:Path"]);
        Assert.Null(configuration["Kestrel:Endpoints:GrpcHttp3:Certificate:Password"]);
    }

    [Fact]
    public void WithTheSettingAbsent_TheKestrelSectionStandsAsWritten()
    {
        var configuration = Configuration(Shipped);

        Assert.Empty(BrokerEndpoints.Resolve(configuration));
    }

    private static IConfigurationRoot Configuration(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();
}
