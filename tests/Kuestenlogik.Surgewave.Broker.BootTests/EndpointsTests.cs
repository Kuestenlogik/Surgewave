using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The shipped broker's two HTTP endpoints, with and without TLS (#276). gRPC is the base of every interface of the
/// broker, and it needs HTTP/2: without TLS a Kestrel endpoint that names HTTP/1.1 and HTTP/2 serves HTTP/1.1 only,
/// so a broker without a certificate answered REST and refused every gRPC call (HTTP_1_1_REQUIRED). And with a
/// certificate the broker gave it to the first endpoint only, so it did not start with the second one on HTTPS
/// ("Unable to configure HTTPS endpoint").
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class EndpointsTests
{
    // TopicService.ListTopics of surgewave.proto, with an empty request: five bytes of gRPC framing and no message.
    private const string ListTopics = "/klsurgewave.TopicService/ListTopics";

    [Fact(Timeout = 180_000)]
    public async Task WithoutTls_RestAnswersOnTheGrpcPort_AndGrpcOnTheSecondEndpoint()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Directory.CreateTempSubdirectory("surgewave-boottest-").FullName;
        try
        {
            await using var broker = await BrokerProcess.StartAsync(dataDir, cancellationToken);
            using var client = new HttpClient();

            using var health = await client.SendAsync(
                Get($"http://127.0.0.1:{broker.RestPort}/health/ready", HttpVersion.Version11), cancellationToken);
            Assert.True(health.IsSuccessStatusCode, $"/health/ready answered {(int)health.StatusCode}\n{broker.Dump()}");

            using var topics = await client.SendAsync(
                GrpcCall($"http://127.0.0.1:{broker.SecondEndpointPort}{ListTopics}"), cancellationToken);
            await topics.Content.ReadAsByteArrayAsync(cancellationToken);
            Assert.Equal(HttpVersion.Version20, topics.Version);
            Assert.True(GrpcStatus(topics) == "0", $"ListTopics answered grpc-status {GrpcStatus(topics)}\n{broker.Dump()}");

            // The broker says at every start that it runs without TLS and where gRPC is then.
            Assert.Contains($"gRPC over HTTP/2 without TLS on port {broker.SecondEndpointPort}", broker.Log, StringComparison.Ordinal);
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    [Fact(Timeout = 180_000)]
    public async Task WithTlsAndACertificate_BothEndpointsAnswerOverHttpsWithIt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Directory.CreateTempSubdirectory("surgewave-boottest-").FullName;
        try
        {
            const string password = "boot-test";
            var certificateFile = Path.Combine(dataDir, "broker.pfx");
            using var certificate = SelfSigned();
            await File.WriteAllBytesAsync(certificateFile, certificate.Export(X509ContentType.Pfx, password), cancellationToken);

            await using var broker = await BrokerProcess.StartAsync(dataDir, cancellationToken,
                "--Surgewave:GrpcUseTls=true",
                $"--Surgewave:GrpcCertificatePath={certificateFile}",
                $"--Surgewave:GrpcCertificatePassword={password}");

            using var handler = new SocketsHttpHandler();
            handler.SslOptions.RemoteCertificateValidationCallback = (_, presented, _, _) =>
                presented is not null && presented.GetCertHashString() == certificate.GetCertHashString();
            using var client = new HttpClient(handler);

            foreach (var port in (int[])[broker.RestPort, broker.SecondEndpointPort])
            {
                using var health = await client.SendAsync(
                    Get($"https://127.0.0.1:{port}/health/ready", HttpVersion.Version11), cancellationToken);
                Assert.True(health.IsSuccessStatusCode,
                    $"/health/ready on port {port} answered {(int)health.StatusCode}\n{broker.Dump()}");

                using var topics = await client.SendAsync(GrpcCall($"https://127.0.0.1:{port}{ListTopics}"), cancellationToken);
                await topics.Content.ReadAsByteArrayAsync(cancellationToken);
                Assert.True(GrpcStatus(topics) == "0",
                    $"ListTopics on port {port} answered grpc-status {GrpcStatus(topics)}\n{broker.Dump()}");
            }
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    private static HttpRequestMessage Get(string url, Version version) =>
        new(HttpMethod.Get, new Uri(url)) { Version = version, VersionPolicy = HttpVersionPolicy.RequestVersionExact };

    // A unary gRPC call as it is on the wire: HTTP/2 - with prior knowledge where there is no TLS to agree on it -
    // and a length-prefixed message, here the empty one.
    private static HttpRequestMessage GrpcCall(string url)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(url))
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(new byte[5]),
        };
        request.Content.Headers.ContentType = new("application/grpc");
        request.Headers.TE.Add(new("trailers"));
        return request;
    }

    // In the trailers of an answer with a message, in the headers of one without.
    private static string GrpcStatus(HttpResponseMessage response) =>
        response.TrailingHeaders.TryGetValues("grpc-status", out var trailer) ? string.Join(",", trailer)
        : response.Headers.TryGetValues("grpc-status", out var header) ? string.Join(",", header)
        : $"missing (HTTP {(int)response.StatusCode})";

    private static X509Certificate2 SelfSigned()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }
}
