using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Covers the <c>Surgewave:GrpcUseTls</c> toggle's configuration surface: binding, defaults,
/// and <see cref="BrokerConfig.Validate"/> cross-property rules. The Kestrel side-effect
/// (endpoint URL override in <c>Program.cs</c>) is exercised by the broker smoke tests
/// that actually start Kestrel; this suite only pins the config contract.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class GrpcTlsConfigTests
{
    [Fact]
    public void Defaults_Leave_Tls_Off()
    {
        var cfg = new BrokerConfig();
        Assert.False(cfg.GrpcUseTls);
        Assert.Null(cfg.GrpcCertificatePath);
        Assert.Null(cfg.GrpcCertificatePassword);
    }

    [Fact]
    public void Validate_Passes_When_Tls_Off_And_No_Cert_Path()
    {
        var cfg = new BrokerConfig
        {
            Host = "h", DataDirectory = "./data", LogDirectory = "./logs",
            Port = 9092, GrpcPort = 9093, ReplicationPort = 9099,
            GrpcUseTls = false,
        };

        var errors = cfg.Validate();
        Assert.DoesNotContain(errors, e => e.Contains("GrpcCertificate", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_Flags_Cert_Path_Without_Tls()
    {
        var cfg = new BrokerConfig
        {
            Host = "h", DataDirectory = "./data", LogDirectory = "./logs",
            Port = 9092, GrpcPort = 9093, ReplicationPort = 9099,
            GrpcUseTls = false,
            GrpcCertificatePath = "/tmp/somewhere.pfx",
        };

        var errors = cfg.Validate();
        Assert.Contains(errors, e =>
            e.Contains("GrpcCertificatePath", StringComparison.Ordinal) &&
            e.Contains("GrpcUseTls is false", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_Flags_Missing_Cert_File()
    {
        var cfg = new BrokerConfig
        {
            Host = "h", DataDirectory = "./data", LogDirectory = "./logs",
            Port = 9092, GrpcPort = 9093, ReplicationPort = 9099,
            GrpcUseTls = true,
            GrpcCertificatePath = $"/tmp/does-not-exist-{Guid.NewGuid():N}.pfx",
        };

        var errors = cfg.Validate();
        Assert.Contains(errors, e => e.Contains("does not exist", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_Passes_With_Valid_Cert_File()
    {
        var tempCert = Path.GetTempFileName();
        try
        {
            var cfg = new BrokerConfig
            {
                Host = "h", DataDirectory = "./data", LogDirectory = "./logs",
                Port = 9092, GrpcPort = 9093, ReplicationPort = 9099,
                GrpcUseTls = true,
                GrpcCertificatePath = tempCert,
                GrpcCertificatePassword = "changeit",
            };

            var errors = cfg.Validate();
            Assert.DoesNotContain(errors, e => e.Contains("GrpcCertificate", StringComparison.Ordinal));
        }
        finally
        {
            File.Delete(tempCert);
        }
    }

    [Fact]
    public void Validate_Passes_With_Tls_And_No_Cert_Path()
    {
        // Dev-cert path: GrpcUseTls=true + no cert path is the expected local-dev shape.
        var cfg = new BrokerConfig
        {
            Host = "h", DataDirectory = "./data", LogDirectory = "./logs",
            Port = 9092, GrpcPort = 9093, ReplicationPort = 9099,
            GrpcUseTls = true,
        };

        var errors = cfg.Validate();
        Assert.DoesNotContain(errors, e => e.Contains("GrpcCertificate", StringComparison.Ordinal));
    }
}
