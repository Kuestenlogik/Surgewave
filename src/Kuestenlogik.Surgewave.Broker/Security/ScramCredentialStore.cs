using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Stores SCRAM credentials with pre-computed keys for efficient authentication.
/// Credentials are stored with ServerKey and StoredKey per RFC 5802.
/// </summary>
public sealed class ScramCredentialStore
{
    private readonly ConcurrentDictionary<string, ScramCredential> _credentials = new(StringComparer.OrdinalIgnoreCase);
    private readonly string? _credentialsFilePath;
    private readonly int _defaultIterations;
    private readonly HashAlgorithmName _hashAlgorithm;
    private readonly ScramCredential _fakeCredential;

    public ScramCredentialStore(
        string? credentialsFilePath = null,
        HashAlgorithmName? hashAlgorithm = null,
        int iterations = 4096)
    {
        _credentialsFilePath = credentialsFilePath;
        _defaultIterations = iterations;
        _hashAlgorithm = hashAlgorithm ?? HashAlgorithmName.SHA256;

        // Create fake credential to prevent timing attacks during username enumeration
        _fakeCredential = CreateCredentialInternal("_fake_", "randompassword12345!");

        if (!string.IsNullOrEmpty(credentialsFilePath) && File.Exists(credentialsFilePath))
        {
            LoadFromFile(credentialsFilePath);
        }
    }

    /// <summary>
    /// Add or update a user credential from plaintext password.
    /// Computes and stores SaltedPassword, StoredKey, and ServerKey.
    /// </summary>
    public void AddUser(string username, string password)
    {
        var credential = CreateCredentialInternal(username, password);
        _credentials[username] = credential;
    }

    /// <summary>
    /// Add a pre-computed credential (e.g., loaded from file)
    /// </summary>
    public void AddCredential(ScramCredential credential)
    {
        _credentials[credential.Username] = credential;
    }

    /// <summary>
    /// Try to get stored credential for a user
    /// </summary>
    public bool TryGetCredential(string username, out ScramCredential credential)
    {
        return _credentials.TryGetValue(username, out credential!);
    }

    /// <summary>
    /// Fake credential for non-existent users (prevents timing attacks)
    /// </summary>
    public ScramCredential FakeCredential => _fakeCredential;

    /// <summary>
    /// Check if a user exists
    /// </summary>
    public bool UserExists(string username) => _credentials.ContainsKey(username);

    /// <summary>
    /// Remove a user
    /// </summary>
    public bool RemoveUser(string username) => _credentials.TryRemove(username, out _);

    /// <summary>
    /// List all usernames
    /// </summary>
    public IEnumerable<string> ListUsers() => _credentials.Keys;

    /// <summary>
    /// Save credentials to file for persistence
    /// </summary>
    public void SaveToFile(string? path = null)
    {
        var filePath = path ?? _credentialsFilePath
            ?? throw new InvalidOperationException("No credentials file path specified");

        var lines = _credentials.Values.Select(c =>
            $"{c.Username}:{Convert.ToBase64String(c.Salt)}:{c.Iterations}:" +
            $"{Convert.ToBase64String(c.StoredKey)}:{Convert.ToBase64String(c.ServerKey)}");

        File.WriteAllLines(filePath, lines);
    }

    private void LoadFromFile(string path)
    {
        foreach (var line in File.ReadAllLines(path))
        {
            if (string.IsNullOrWhiteSpace(line) || line.StartsWith('#'))
                continue;

            var parts = line.Split(':');
            if (parts.Length >= 5)
            {
                _credentials[parts[0]] = new ScramCredential
                {
                    Username = parts[0],
                    Salt = Convert.FromBase64String(parts[1]),
                    Iterations = int.Parse(parts[2]),
                    StoredKey = Convert.FromBase64String(parts[3]),
                    ServerKey = Convert.FromBase64String(parts[4])
                };
            }
        }
    }

    private ScramCredential CreateCredentialInternal(string username, string password)
    {
        var salt = new byte[16];
        RandomNumberGenerator.Fill(salt);

        // SaltedPassword = Hi(Normalize(password), salt, i)
        // Hi is PBKDF2
        var saltedPassword = ComputeSaltedPassword(password, salt, _defaultIterations);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var clientKey = ComputeHmac(saltedPassword, "Client Key"u8.ToArray());

        // StoredKey = H(ClientKey)
        var storedKey = ComputeHash(clientKey);

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var serverKey = ComputeHmac(saltedPassword, "Server Key"u8.ToArray());

        return new ScramCredential
        {
            Username = username,
            Salt = salt,
            Iterations = _defaultIterations,
            StoredKey = storedKey,
            ServerKey = serverKey
        };
    }

    private byte[] ComputeSaltedPassword(string password, byte[] salt, int iterations)
    {
        return Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password),
            salt,
            iterations,
            _hashAlgorithm,
            _hashAlgorithm.Name == "SHA512" ? 64 : 32);
    }

    private byte[] ComputeHmac(byte[] key, byte[] data)
    {
        return _hashAlgorithm.Name switch
        {
            "SHA256" => HMACSHA256.HashData(key, data),
            "SHA512" => HMACSHA512.HashData(key, data),
            _ => throw new InvalidOperationException($"Unsupported hash algorithm: {_hashAlgorithm.Name}")
        };
    }

    private byte[] ComputeHash(byte[] data)
    {
        return _hashAlgorithm.Name switch
        {
            "SHA256" => SHA256.HashData(data),
            "SHA512" => SHA512.HashData(data),
            _ => throw new InvalidOperationException($"Unsupported hash algorithm: {_hashAlgorithm.Name}")
        };
    }
}

/// <summary>
/// SCRAM credential containing pre-computed authentication keys
/// </summary>
public sealed class ScramCredential
{
    public required string Username { get; init; }
    public required byte[] Salt { get; init; }
    public required int Iterations { get; init; }
    public required byte[] StoredKey { get; init; }
    public required byte[] ServerKey { get; init; }
}
