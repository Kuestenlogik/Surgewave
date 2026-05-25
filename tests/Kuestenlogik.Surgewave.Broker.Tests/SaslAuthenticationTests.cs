using System.Text;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for SASL authentication components (CredentialStore, SaslAuthenticator, ConnectionState).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SaslAuthenticationTests
{
    #region CredentialStore Tests

    [Fact]
    public void CredentialStore_AddAndValidateUser_Success()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");

        // Act
        var isValid = store.ValidateCredentials("testuser", "testpassword");

        // Assert
        Assert.True(isValid);
    }

    [Fact]
    public void CredentialStore_ValidateUser_WrongPassword_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");

        // Act
        var isValid = store.ValidateCredentials("testuser", "wrongpassword");

        // Assert
        Assert.False(isValid);
    }

    [Fact]
    public void CredentialStore_ValidateUser_UnknownUser_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");

        // Act
        var isValid = store.ValidateCredentials("unknownuser", "testpassword");

        // Assert
        Assert.False(isValid);
    }

    [Fact]
    public void CredentialStore_UserExists_ReturnsCorrectValue()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("existinguser", "password");

        // Act & Assert
        Assert.True(store.UserExists("existinguser"));
        Assert.False(store.UserExists("nonexistentuser"));
    }

    [Fact]
    public void CredentialStore_RemoveUser_Success()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");

        // Act
        var removed = store.RemoveUser("testuser");

        // Assert
        Assert.True(removed);
        Assert.False(store.UserExists("testuser"));
    }

    [Fact]
    public void CredentialStore_ListUsers_ReturnsAllUsers()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("user1", "password1");
        store.AddUser("user2", "password2");
        store.AddUser("user3", "password3");

        // Act
        var users = store.ListUsers().ToList();

        // Assert
        Assert.Equal(3, users.Count);
        Assert.Contains("user1", users);
        Assert.Contains("user2", users);
        Assert.Contains("user3", users);
    }

    [Fact]
    public void CredentialStore_UpdateUser_OverwritesCredentials()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "oldpassword");

        // Act
        store.AddUser("testuser", "newpassword");

        // Assert
        Assert.False(store.ValidateCredentials("testuser", "oldpassword"));
        Assert.True(store.ValidateCredentials("testuser", "newpassword"));
    }

    [Fact]
    public void CredentialStore_CaseInsensitiveUsernames()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("TestUser", "password");

        // Act & Assert - username lookup should be case-insensitive
        Assert.True(store.UserExists("testuser"));
        Assert.True(store.UserExists("TESTUSER"));
        Assert.True(store.ValidateCredentials("testuser", "password"));
    }

    #endregion

    #region SaslAuthenticator Tests

    [Fact]
    public void SaslAuthenticator_DefaultMechanism_IsPlain()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store);

        // Act
        var mechanisms = authenticator.EnabledMechanisms;

        // Assert
        Assert.Single(mechanisms);
        Assert.Contains("PLAIN", mechanisms);
    }

    [Fact]
    public void SaslAuthenticator_IsMechanismSupported_Plain_True()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store);

        // Act & Assert
        Assert.True(authenticator.IsMechanismSupported("PLAIN"));
        Assert.True(authenticator.IsMechanismSupported("plain")); // case insensitive
    }

    [Fact]
    public void SaslAuthenticator_IsMechanismSupported_Unknown_False()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store);

        // Act & Assert
        Assert.False(authenticator.IsMechanismSupported("UNKNOWN"));
        Assert.False(authenticator.IsMechanismSupported("SCRAM-SHA-256")); // not enabled by default
    }

    [Fact]
    public void SaslAuthenticator_PlainAuth_ValidCredentials_Success()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");
        var authenticator = new SaslAuthenticator(store);

        // SASL PLAIN format: [authzid] NUL username NUL password
        var authBytes = CreatePlainAuthBytes("", "testuser", "testpassword");

        // Act
        var result = authenticator.Authenticate("PLAIN", authBytes);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal("testuser", result.Username);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void SaslAuthenticator_PlainAuth_InvalidPassword_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");
        var authenticator = new SaslAuthenticator(store);

        var authBytes = CreatePlainAuthBytes("", "testuser", "wrongpassword");

        // Act
        var result = authenticator.Authenticate("PLAIN", authBytes);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Null(result.Username);
        Assert.NotNull(result.ErrorMessage);
    }

    [Fact]
    public void SaslAuthenticator_PlainAuth_UnknownUser_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");
        var authenticator = new SaslAuthenticator(store);

        var authBytes = CreatePlainAuthBytes("", "unknownuser", "testpassword");

        // Act
        var result = authenticator.Authenticate("PLAIN", authBytes);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Null(result.Username);
    }

    [Fact]
    public void SaslAuthenticator_PlainAuth_WithAuthzId_Success()
    {
        // Arrange
        var store = new CredentialStore();
        store.AddUser("testuser", "testpassword");
        var authenticator = new SaslAuthenticator(store);

        // SASL PLAIN with authzid (authorization identity)
        var authBytes = CreatePlainAuthBytes("authzid", "testuser", "testpassword");

        // Act
        var result = authenticator.Authenticate("PLAIN", authBytes);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal("testuser", result.Username);
    }

    [Fact]
    public void SaslAuthenticator_PlainAuth_EmptyUsername_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store);

        var authBytes = CreatePlainAuthBytes("", "", "password");

        // Act
        var result = authenticator.Authenticate("PLAIN", authBytes);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("empty", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SaslAuthenticator_UnsupportedMechanism_Fails()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store);

        // Act
        var result = authenticator.Authenticate("UNKNOWN", []);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("Unsupported", result.ErrorMessage!);
    }

    [Fact]
    public void SaslAuthenticator_CustomMechanisms_Respected()
    {
        // Arrange
        var store = new CredentialStore();
        var authenticator = new SaslAuthenticator(store, ["PLAIN", "SCRAM-SHA-256"]);

        // Act
        var mechanisms = authenticator.EnabledMechanisms;

        // Assert
        Assert.Equal(2, mechanisms.Length);
        Assert.Contains("PLAIN", mechanisms);
        Assert.Contains("SCRAM-SHA-256", mechanisms);
    }

    #endregion

    #region ConnectionState Tests

    [Fact]
    public void ConnectionState_Initial_NotAuthenticated()
    {
        // Arrange & Act
        var state = new ConnectionState("127.0.0.1");

        // Assert
        Assert.False(state.IsAuthenticated);
        Assert.Null(state.AuthenticatedUser);
        Assert.Null(state.SaslMechanism);
        Assert.Null(state.NegotiatedMechanism);
        Assert.Null(state.AuthenticatedAt);
    }

    [Fact]
    public void ConnectionState_SetNegotiatedMechanism_UpdatesState()
    {
        // Arrange
        var state = new ConnectionState("127.0.0.1");

        // Act
        state.SetNegotiatedMechanism("PLAIN");

        // Assert
        Assert.Equal("PLAIN", state.NegotiatedMechanism);
        Assert.False(state.IsAuthenticated); // not yet authenticated
    }

    [Fact]
    public void ConnectionState_SetAuthenticated_UpdatesState()
    {
        // Arrange
        var state = new ConnectionState("127.0.0.1");

        // Act
        state.SetAuthenticated("testuser", "PLAIN");

        // Assert
        Assert.True(state.IsAuthenticated);
        Assert.Equal("testuser", state.AuthenticatedUser);
        Assert.Equal("PLAIN", state.SaslMechanism);
        Assert.NotNull(state.AuthenticatedAt);
    }

    [Fact]
    public void ConnectionState_Reset_ClearsState()
    {
        // Arrange
        var state = new ConnectionState("127.0.0.1");
        state.SetNegotiatedMechanism("PLAIN");
        state.SetAuthenticated("testuser", "PLAIN");

        // Act
        state.Reset();

        // Assert
        Assert.False(state.IsAuthenticated);
        Assert.Null(state.AuthenticatedUser);
        Assert.Null(state.SaslMechanism);
        Assert.Null(state.NegotiatedMechanism);
        Assert.Null(state.AuthenticatedAt);
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Create SASL PLAIN authentication bytes
    /// Format: authzid NUL username NUL password
    /// </summary>
    private static byte[] CreatePlainAuthBytes(string authzId, string username, string password)
    {
        using var ms = new MemoryStream();

        // Write authzid
        if (!string.IsNullOrEmpty(authzId))
        {
            ms.Write(Encoding.UTF8.GetBytes(authzId));
        }
        ms.WriteByte(0); // NUL separator

        // Write username
        ms.Write(Encoding.UTF8.GetBytes(username));
        ms.WriteByte(0); // NUL separator

        // Write password
        ms.Write(Encoding.UTF8.GetBytes(password));

        return ms.ToArray();
    }

    #endregion
}
