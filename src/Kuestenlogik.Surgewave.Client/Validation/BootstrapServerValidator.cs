using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Client.Validation;

/// <summary>
/// Validates bootstrap server addresses (host:port format).
/// </summary>
public static partial class BootstrapServerValidator
{
    /// <summary>
    /// Validates a bootstrap server address.
    /// </summary>
    /// <param name="server">The server address to validate (host:port format).</param>
    /// <returns>A validation result indicating success or the error message.</returns>
    public static ValidationResult Validate(string? server)
    {
        if (string.IsNullOrWhiteSpace(server))
        {
            return ValidationResult.Error("server address cannot be null or empty");
        }

        // Check for IPv6 with port: [::1]:9092
        if (server.StartsWith('['))
        {
            var closeBracket = server.IndexOf(']');
            if (closeBracket == -1)
            {
                return ValidationResult.Error("invalid IPv6 address format - missing closing bracket");
            }

            // If there's a port, it should be after ]
            if (closeBracket < server.Length - 1)
            {
                if (server[closeBracket + 1] != ':')
                {
                    return ValidationResult.Error("invalid format - port separator expected after IPv6 address");
                }

                var portStr = server[(closeBracket + 2)..];
                if (!ValidatePort(portStr, out var portError))
                {
                    return ValidationResult.Error(portError);
                }
            }

            return ValidationResult.Success;
        }

        // Standard host:port format
        var colonIndex = server.LastIndexOf(':');
        if (colonIndex == -1)
        {
            // No port specified - that's okay, we'll use default
            if (!ValidateHost(server, out var hostError))
            {
                return ValidationResult.Error(hostError);
            }
            return ValidationResult.Success;
        }

        var host = server[..colonIndex];
        var port = server[(colonIndex + 1)..];

        if (!ValidateHost(host, out var hostErr))
        {
            return ValidationResult.Error(hostErr);
        }

        if (!ValidatePort(port, out var portErr))
        {
            return ValidationResult.Error(portErr);
        }

        return ValidationResult.Success;
    }

    /// <summary>
    /// Returns true if the server address is valid.
    /// </summary>
    public static bool IsValid(string? server) => Validate(server).IsValid;

    private static bool ValidateHost(string host, out string error)
    {
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(host))
        {
            error = "host cannot be empty";
            return false;
        }

        // Allow localhost, IP addresses, and hostnames
        if (!ValidHostRegex().IsMatch(host))
        {
            error = "invalid host format";
            return false;
        }

        return true;
    }

    private static bool ValidatePort(string portStr, out string error)
    {
        error = string.Empty;

        if (!int.TryParse(portStr, out var port))
        {
            error = "port must be a number";
            return false;
        }

        if (port < 1 || port > 65535)
        {
            error = "port must be between 1 and 65535";
            return false;
        }

        return true;
    }

    // Matches hostnames, IPv4 addresses, and simple patterns
    [GeneratedRegex(@"^[a-zA-Z0-9]([a-zA-Z0-9\-\.]*[a-zA-Z0-9])?$|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", RegexOptions.Compiled)]
    private static partial Regex ValidHostRegex();
}
