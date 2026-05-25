using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// Tests for FailureDomainValidator.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class FailureDomainValidatorTests
{
    private readonly ClusterState _clusterState;

    public FailureDomainValidatorTests()
    {
        _clusterState = new ClusterState();
    }

    [Fact]
    public void ValidateAssignment_EmptyReplicas_ReturnsNoViolations()
    {
        // Arrange
        var validator = new FailureDomainValidator(_clusterState);

        // Act
        var violations = validator.ValidateAssignment([]);

        // Assert
        Assert.Empty(violations);
    }

    [Fact]
    public void ValidateAssignment_SingleReplica_ReturnsNoViolations()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        var validator = new FailureDomainValidator(_clusterState);

        // Act
        var violations = validator.ValidateAssignment([1], "test-topic", 0);

        // Assert
        Assert.Empty(violations);
    }

    [Fact]
    public void ValidateAssignment_ReplicasInDifferentRacks_ReturnsNoViolations()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");
        var validator = new FailureDomainValidator(_clusterState);

        // Act
        var violations = validator.ValidateAssignment([1, 2, 3], "test-topic", 0);

        // Assert
        Assert.Empty(violations);
    }

    [Fact]
    public void ValidateAssignment_AllReplicasSameRack_ReturnsSingleDomainViolation()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-1");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            PreventSingleDomainReplicas = true
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2, 3], "test-topic", 0);

        // Assert
        Assert.Single(violations);
        Assert.Equal(ViolationType.SingleDomainReplicas, violations[0].Type);
        Assert.Equal("test-topic", violations[0].Topic);
        Assert.Equal(0, violations[0].Partition);
    }

    [Fact]
    public void ValidateAssignment_InsufficientDomainSpread_ReturnsViolation()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 3,
            ValidationLevel = FailureDomainLevel.Rack
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2, 3], "test-topic", 0);

        // Assert
        Assert.Single(violations);
        Assert.Equal(ViolationType.InsufficientDomainSpread, violations[0].Type);
        Assert.Equal(3, violations[0].RequiredDomains);
        Assert.Equal(2, violations[0].ActualDomains);
    }

    [Fact]
    public void ValidateAssignment_HierarchicalRack_ValidatesAtZoneLevel()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc1/zone-a/rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-east/dc1/zone-b/rack-3");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            ValidationLevel = FailureDomainLevel.Zone,
            MinDistinctDomains = 2
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2, 3], "test-topic", 0);

        // Assert
        Assert.Empty(violations); // 2 distinct zones (zone-a and zone-b)
    }

    [Fact]
    public void ValidateAssignment_HierarchicalRack_AllSameZone_ReturnsViolation()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc1/zone-a/rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-east/dc1/zone-a/rack-3");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            ValidationLevel = FailureDomainLevel.Zone,
            PreventSingleDomainReplicas = true
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2, 3], "test-topic", 0);

        // Assert
        Assert.Single(violations);
        Assert.Equal(ViolationType.SingleDomainReplicas, violations[0].Type);
        Assert.Equal(FailureDomainLevel.Zone, violations[0].Level);
    }

    [Fact]
    public void ValidateTopicCreation_SufficientDomains_ReturnsNoViolations()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 2
        });

        // Act
        var violations = validator.ValidateTopicCreation(3, "new-topic");

        // Assert
        Assert.Empty(violations);
    }

    [Fact]
    public void ValidateTopicCreation_InsufficientDomains_ReturnsViolation()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 3,
            WarnOnInsufficientDomains = false // Disable warning to get only one violation
        });

        // Act
        var violations = validator.ValidateTopicCreation(2, "new-topic");

        // Assert
        Assert.Single(violations);
        Assert.Equal(ViolationType.InsufficientDomainsAvailable, violations[0].Type);
        Assert.Equal(3, violations[0].RequiredDomains);
        Assert.Equal(1, violations[0].ActualDomains);
    }

    [Fact]
    public void ValidateTopicCreation_ReplicationFactorExceedsDomains_ReturnsWarning()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            WarnOnInsufficientDomains = true
        });

        // Act
        var violations = validator.ValidateTopicCreation(3, "new-topic");

        // Assert
        Assert.Single(violations);
        Assert.Equal(ViolationType.InsufficientDomainsAvailable, violations[0].Type);
        Assert.Contains("Multiple replicas will be placed in the same failure domain", violations[0].Message);
    }

    [Fact]
    public void ValidatorOptions_DefaultValues()
    {
        // Act
        var options = new FailureDomainValidatorOptions();

        // Assert
        Assert.Equal(FailureDomainLevel.Rack, options.ValidationLevel);
        Assert.Equal(0, options.MinDistinctDomains);
        Assert.True(options.PreventSingleDomainReplicas);
        Assert.True(options.WarnOnInsufficientDomains);
    }

    [Fact]
    public void ValidatorOptions_CustomValues()
    {
        // Act
        var options = new FailureDomainValidatorOptions
        {
            ValidationLevel = FailureDomainLevel.Zone,
            MinDistinctDomains = 3,
            PreventSingleDomainReplicas = false,
            WarnOnInsufficientDomains = false
        };

        // Assert
        Assert.Equal(FailureDomainLevel.Zone, options.ValidationLevel);
        Assert.Equal(3, options.MinDistinctDomains);
        Assert.False(options.PreventSingleDomainReplicas);
        Assert.False(options.WarnOnInsufficientDomains);
    }

    [Fact]
    public void FailureDomainViolation_RecordProperties()
    {
        // Act
        var violation = new FailureDomainViolation
        {
            Type = ViolationType.InsufficientDomainSpread,
            Message = "Test message",
            Topic = "test-topic",
            Partition = 5,
            RequiredDomains = 3,
            ActualDomains = 1,
            Level = FailureDomainLevel.Zone
        };

        // Assert
        Assert.Equal(ViolationType.InsufficientDomainSpread, violation.Type);
        Assert.Equal("Test message", violation.Message);
        Assert.Equal("test-topic", violation.Topic);
        Assert.Equal(5, violation.Partition);
        Assert.Equal(3, violation.RequiredDomains);
        Assert.Equal(1, violation.ActualDomains);
        Assert.Equal(FailureDomainLevel.Zone, violation.Level);
    }

    [Fact]
    public void ValidateAssignment_NoRackInfo_UsesDefaultDomain()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, null);
        _clusterState.RegisterBroker(2, "localhost", 9093, null);
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            PreventSingleDomainReplicas = true
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2], "test-topic", 0);

        // Assert - All in "default" domain
        Assert.Single(violations);
        Assert.Equal(ViolationType.SingleDomainReplicas, violations[0].Type);
    }

    [Fact]
    public void ViolationType_AllValues()
    {
        // Act & Assert
        Assert.Equal(0, (int)ViolationType.InsufficientDomainSpread);
        Assert.Equal(1, (int)ViolationType.InsufficientDomainsAvailable);
        Assert.Equal(2, (int)ViolationType.SingleDomainReplicas);
        Assert.Equal(3, (int)ViolationType.ConstraintViolation);
    }

    [Fact]
    public void FailureDomainLevel_Ordering()
    {
        // Assert - Levels are ordered from most granular to least
        Assert.True(FailureDomainLevel.Rack < FailureDomainLevel.Zone);
        Assert.True(FailureDomainLevel.Zone < FailureDomainLevel.Datacenter);
        Assert.True(FailureDomainLevel.Datacenter < FailureDomainLevel.Region);
    }

    [Fact]
    public void ValidateAssignment_DisabledValidation_ReturnsNoViolations()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 0,
            PreventSingleDomainReplicas = false
        });

        // Act
        var violations = validator.ValidateAssignment([1, 2], "test-topic", 0);

        // Assert
        Assert.Empty(violations);
    }
}
