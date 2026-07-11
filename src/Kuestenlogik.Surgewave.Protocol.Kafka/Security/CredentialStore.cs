using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Stores and validates user credentials for SASL authentication.
/// Supports in-memory storage with optional file persistence.
/// </summary>
public sealed class CredentialStore
{
    private readonly ConcurrentDictionary<string, UserCredential> _users = new(StringComparer.OrdinalIgnoreCase);
    private readonly string? _credentialsFilePath;

    public CredentialStore(string? credentialsFilePath = null)
    {
        _credentialsFilePath = credentialsFilePath;

        if (!string.IsNullOrEmpty(credentialsFilePath) && File.Exists(credentialsFilePath))
        {
            LoadFromFile(credentialsFilePath);
        }
    }

    /// <summary>
    /// Add or update a user with plaintext password (will be hashed)
    /// </summary>
    public void AddUser(string username, string password)
    {
        var salt = GenerateSalt();
        var hash = HashPassword(password, salt);

        _users[username] = new UserCredential
        {
            Username = username,
            PasswordHash = hash,
            Salt = salt,
            CreatedAt = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Validate user credentials
    /// </summary>
    public bool ValidateCredentials(string username, string password)
    {
        if (!_users.TryGetValue(username, out var credential))
        {
            return false;
        }

        var hash = HashPassword(password, credential.Salt);
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(hash),
            Encoding.UTF8.GetBytes(credential.PasswordHash));
    }

    /// <summary>
    /// Check if a user exists
    /// </summary>
    public bool UserExists(string username)
    {
        return _users.ContainsKey(username);
    }

    /// <summary>
    /// Remove a user
    /// </summary>
    public bool RemoveUser(string username)
    {
        return _users.TryRemove(username, out _);
    }

    /// <summary>
    /// List all usernames
    /// </summary>
    public IEnumerable<string> ListUsers()
    {
        return _users.Keys;
    }

    /// <summary>
    /// Save credentials to file (for persistence)
    /// </summary>
    public void SaveToFile(string? path = null)
    {
        var filePath = path ?? _credentialsFilePath;
        if (string.IsNullOrEmpty(filePath))
        {
            throw new InvalidOperationException("No credentials file path specified");
        }

        var lines = _users.Values.Select(u =>
            $"{u.Username}:{u.PasswordHash}:{Convert.ToBase64String(u.Salt)}");

        File.WriteAllLines(filePath, lines);
    }

    private void LoadFromFile(string path)
    {
        foreach (var line in File.ReadAllLines(path))
        {
            var parts = line.Split(':');
            if (parts.Length >= 3)
            {
                _users[parts[0]] = new UserCredential
                {
                    Username = parts[0],
                    PasswordHash = parts[1],
                    Salt = Convert.FromBase64String(parts[2]),
                    CreatedAt = DateTime.UtcNow
                };
            }
        }
    }

    private static byte[] GenerateSalt()
    {
        var salt = new byte[16];
        RandomNumberGenerator.Fill(salt);
        return salt;
    }

    private static string HashPassword(string password, byte[] salt)
    {
        var hash = Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            iterations: 100000,
            HashAlgorithmName.SHA256,
            outputLength: 32);

        return Convert.ToBase64String(hash);
    }

    private sealed class UserCredential
    {
        public required string Username { get; init; }
        public required string PasswordHash { get; init; }
        public required byte[] Salt { get; init; }
        public required DateTime CreatedAt { get; init; }
    }
}
