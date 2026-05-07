using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the AclAuthorizer class.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class AclAuthorizerTests
{
    #region Basic Authorization Tests

    [Fact]
    public void Authorize_NoAcls_DenyByDefault()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.False(result.IsAllowed);
        Assert.Contains("No ACL found", result.Reason);
    }

    [Fact]
    public void Authorize_NoAcls_AllowIfConfigured()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: true);

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
        Assert.Contains("default allow", result.Reason);
    }

    [Fact]
    public void Authorize_AllowAcl_Succeeds()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        });

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
    }

    [Fact]
    public void Authorize_DenyAcl_Denied()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: true);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Deny
        });

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.False(result.IsAllowed);
        Assert.Contains("Denied by ACL", result.Reason);
    }

    [Fact]
    public void Authorize_DenyTakesPrecedenceOverAllow()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        });
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Deny
        });

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.False(result.IsAllowed);
        Assert.Contains("Denied by ACL", result.Reason);
    }

    #endregion

    #region Super User Tests

    [Fact]
    public void Authorize_SuperUser_AlwaysAllowed()
    {
        // Arrange
        var authorizer = new AclAuthorizer(
            allowIfNoAclFound: false,
            superUsers: ["User:admin"]);

        // Act - admin should be allowed even without any ACLs
        var result = authorizer.Authorize(
            "User:admin",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
        Assert.Contains("Super user", result.Reason);
    }

    [Fact]
    public void Authorize_SuperUser_BypassesDenyAcl()
    {
        // Arrange
        var authorizer = new AclAuthorizer(
            allowIfNoAclFound: false,
            superUsers: ["User:admin"]);

        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:admin",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Deny
        });

        // Act - super user should bypass even explicit deny
        var result = authorizer.Authorize(
            "User:admin",
            "*",
            AclResourceType.Topic,
            "test-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
    }

    #endregion

    #region Pattern Matching Tests

    [Fact]
    public void Authorize_WildcardPrincipal_MatchesAll()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:*",
            ResourceType = AclResourceType.Topic,
            ResourceName = "public-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        });

        // Act
        var result = authorizer.Authorize(
            "User:anyuser",
            "*",
            AclResourceType.Topic,
            "public-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
    }

    [Fact]
    public void Authorize_WildcardResource_MatchesAll()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "*",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        });

        // Act
        var result = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "any-topic",
            AclOperation.Read);

        // Assert
        Assert.True(result.IsAllowed);
    }

    [Fact]
    public void Authorize_PrefixedPattern_MatchesPrefix()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "team-a-",
            PatternType = AclPatternType.Prefixed,
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        });

        // Act - topic with matching prefix
        var result1 = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "team-a-events",
            AclOperation.Read);

        // topic without matching prefix
        var result2 = authorizer.Authorize(
            "User:alice",
            "*",
            AclResourceType.Topic,
            "team-b-events",
            AclOperation.Read);

        // Assert
        Assert.True(result1.IsAllowed);
        Assert.False(result2.IsAllowed);
    }

    [Fact]
    public void Authorize_AllOperation_MatchesAnyOperation()
    {
        // Arrange
        var authorizer = new AclAuthorizer(allowIfNoAclFound: false);
        authorizer.AddAcl(new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.All,
            Permission = AclPermission.Allow
        });

        // Act
        var readResult = authorizer.Authorize(
            "User:alice", "*", AclResourceType.Topic, "test-topic", AclOperation.Read);
        var writeResult = authorizer.Authorize(
            "User:alice", "*", AclResourceType.Topic, "test-topic", AclOperation.Write);
        var describeResult = authorizer.Authorize(
            "User:alice", "*", AclResourceType.Topic, "test-topic", AclOperation.Describe);

        // Assert
        Assert.True(readResult.IsAllowed);
        Assert.True(writeResult.IsAllowed);
        Assert.True(describeResult.IsAllowed);
    }

    #endregion

    #region ACL Management Tests

    [Fact]
    public void AddAcl_IncreasesAclCount()
    {
        // Arrange
        var authorizer = new AclAuthorizer();

        // Act
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicRead("User:alice", "test-topic"));
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicWrite("User:bob", "test-topic"));

        // Assert
        Assert.Equal(2, authorizer.AclCount);
    }

    [Fact]
    public void ListAcls_ReturnsAllAcls()
    {
        // Arrange
        var authorizer = new AclAuthorizer();
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicRead("User:alice", "test-topic"));
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicWrite("User:bob", "test-topic"));

        // Act
        var acls = authorizer.ListAcls().ToList();

        // Assert
        Assert.Equal(2, acls.Count);
    }

    [Fact]
    public void ListAcls_WithFilter_ReturnsMatchingAcls()
    {
        // Arrange
        var authorizer = new AclAuthorizer();
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicRead("User:alice", "test-topic"));
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicWrite("User:bob", "test-topic"));

        // Act
        var aliceAcls = authorizer.ListAcls(a => a.Principal == "User:alice").ToList();

        // Assert
        Assert.Single(aliceAcls);
        Assert.Equal("User:alice", aliceAcls[0].Principal);
    }

    [Fact]
    public void RemoveAcls_RemovesMatchingAcls()
    {
        // Arrange
        var authorizer = new AclAuthorizer();
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicRead("User:alice", "test-topic"));
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicWrite("User:alice", "test-topic"));
        authorizer.AddAcl(AclAuthorizer.CommonAcls.AllowTopicRead("User:bob", "test-topic"));

        // Act
        var removed = authorizer.RemoveAcls(a => a.Principal == "User:alice");

        // Assert
        Assert.Equal(2, removed);
        Assert.Equal(1, authorizer.AclCount);
    }

    #endregion

    #region Common ACLs Helper Tests

    [Fact]
    public void CommonAcls_AllowTopicRead_CreatesCorrectAcl()
    {
        // Act
        var acl = AclAuthorizer.CommonAcls.AllowTopicRead("User:alice", "test-topic");

        // Assert
        Assert.Equal("User:alice", acl.Principal);
        Assert.Equal(AclResourceType.Topic, acl.ResourceType);
        Assert.Equal("test-topic", acl.ResourceName);
        Assert.Equal(AclOperation.Read, acl.Operation);
        Assert.Equal(AclPermission.Allow, acl.Permission);
    }

    [Fact]
    public void CommonAcls_AllowTopicPrefixRead_CreatesCorrectAcl()
    {
        // Act
        var acl = AclAuthorizer.CommonAcls.AllowTopicPrefixRead("User:alice", "team-a-");

        // Assert
        Assert.Equal(AclPatternType.Prefixed, acl.PatternType);
    }

    #endregion

    #region AclEntry Tests

    [Fact]
    public void AclEntry_Matches_CaseInsensitive()
    {
        // Arrange
        var acl = new AclEntry
        {
            Principal = "User:Alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "Test-Topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        // Act
        var matches = acl.Matches(
            "user:alice",  // different case
            "*",
            AclResourceType.Topic,
            "test-topic",  // different case
            AclOperation.Read);

        // Assert
        Assert.True(matches);
    }

    [Fact]
    public void AclEntry_ToString_ReturnsReadableString()
    {
        // Arrange
        var acl = new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        // Act
        var str = acl.ToString();

        // Assert
        Assert.Contains("User:alice", str);
        Assert.Contains("Topic", str);
        Assert.Contains("Read", str);
        Assert.Contains("Allow", str);
    }

    [Fact]
    public void AclEntry_Validate_ValidEntry_ReturnsNull()
    {
        // Arrange
        var acl = new AclEntry
        {
            Principal = "User:alice",
            ResourceType = AclResourceType.Topic,
            ResourceName = "test-topic",
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        // Act
        var error = acl.Validate();

        // Assert
        Assert.Null(error);
    }

    #endregion
}
