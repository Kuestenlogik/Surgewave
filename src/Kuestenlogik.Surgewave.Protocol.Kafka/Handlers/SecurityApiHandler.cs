using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for security APIs: SaslHandshake, SaslAuthenticate, DescribeAcls, CreateAcls, DeleteAcls
/// </summary>
public sealed class SecurityApiHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly SaslAuthenticator? _saslAuthenticator;
    private readonly IAuthorizer? _aclAuthorizer;
    private readonly IAuditLogger? _auditLogger;
    private readonly ScramCredentialStore? _scramSha256Store;
    private readonly ScramCredentialStore? _scramSha512Store;
    private readonly ILogger<SecurityApiHandler> _logger;

    /// <summary>
    /// Mechanism codes per KIP-554 / Kafka admin client: 1 = SCRAM-SHA-256,
    /// 2 = SCRAM-SHA-512. Anything else is an unknown mechanism — handlers
    /// reject the per-row entry rather than guessing.
    /// </summary>
    private const sbyte MechanismScramSha256 = 1;
    private const sbyte MechanismScramSha512 = 2;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.SaslHandshake, ApiKey.SaslAuthenticate,
        ApiKey.DescribeAcls, ApiKey.CreateAcls, ApiKey.DeleteAcls,
        ApiKey.DescribeUserScramCredentials, ApiKey.AlterUserScramCredentials,
    ];

    public SecurityApiHandler(
        IBrokerConfigView config,
        SaslAuthenticator? saslAuthenticator,
        IAuthorizer? aclAuthorizer,
        IAuditLogger? auditLogger,
        ILogger<SecurityApiHandler> logger,
        ScramCredentialStore? scramSha256Store = null,
        ScramCredentialStore? scramSha512Store = null)
    {
        _config = config;
        _saslAuthenticator = saslAuthenticator;
        _aclAuthorizer = aclAuthorizer;
        _auditLogger = auditLogger;
        _scramSha256Store = scramSha256Store;
        _scramSha512Store = scramSha512Store;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult<KafkaResponse>(request switch
        {
            SaslHandshakeRequest saslHandshakeRequest => HandleSaslHandshake(saslHandshakeRequest, context.ConnectionState),
            SaslAuthenticateRequest saslAuthenticateRequest => HandleSaslAuthenticate(saslAuthenticateRequest, context.ConnectionState),
            DescribeAclsRequest describeAclsRequest => HandleDescribeAcls(describeAclsRequest),
            CreateAclsRequest createAclsRequest => HandleCreateAcls(createAclsRequest, context),
            DeleteAclsRequest deleteAclsRequest => HandleDeleteAcls(deleteAclsRequest, context),
            DescribeUserScramCredentialsRequest describeScramRequest => HandleDescribeUserScramCredentials(describeScramRequest),
            AlterUserScramCredentialsRequest alterScramRequest => HandleAlterUserScramCredentials(alterScramRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by SecurityApiHandler")
        });
    }

    /// <summary>
    /// KIP-554 list / inspect SCRAM credentials. The user filter is optional —
    /// an empty / null Users list means "list everything". When the broker
    /// runs without SCRAM stores wired (the default — Surgewave boots with PLAIN
    /// + OAUTHBEARER and gives operators the option to add SCRAM via
    /// <c>SaslMechanisms</c>), every requested user surfaces as
    /// <see cref="ErrorCode.ResourceNotFound"/> with a documented reason.
    /// </summary>
    private DescribeUserScramCredentialsResponse HandleDescribeUserScramCredentials(DescribeUserScramCredentialsRequest request)
    {
        var results = new List<DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult>();

        // Build the user set: explicit list or "every user across both stores".
        var requestedUsers = request.Users is { Count: > 0 }
            ? request.Users.Select(u => u.Name).Distinct(StringComparer.Ordinal).ToList()
            : EnumerateAllStoredUsers().ToList();

        foreach (var user in requestedUsers)
        {
            var infos = new List<DescribeUserScramCredentialsResponse.CredentialInfo>();

            if (_scramSha256Store?.TryGetCredential(user, out var c256) == true)
            {
                infos.Add(new DescribeUserScramCredentialsResponse.CredentialInfo
                {
                    Mechanism = MechanismScramSha256,
                    Iterations = c256.Iterations,
                });
            }
            if (_scramSha512Store?.TryGetCredential(user, out var c512) == true)
            {
                infos.Add(new DescribeUserScramCredentialsResponse.CredentialInfo
                {
                    Mechanism = MechanismScramSha512,
                    Iterations = c512.Iterations,
                });
            }

            results.Add(new DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
            {
                User = user,
                CredentialInfos = infos,
                ErrorCode = infos.Count == 0 ? ErrorCode.ResourceNotFound : ErrorCode.None,
                ErrorMessage = infos.Count == 0
                    ? "No SCRAM credentials registered for this user (or SCRAM stores not configured)"
                    : null,
            });
        }

        return new DescribeUserScramCredentialsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Results = results,
        };
    }

    /// <summary>
    /// KIP-554 upsert / delete SCRAM credentials. Each row in the request is
    /// treated independently — the per-row error code communicates outcome
    /// so a partial-success batch is preserved. Surgewave currently persists
    /// credentials in-memory only (the store has a SaveToFile path the
    /// operator can wire up via config); persisting via the AlterUser flow
    /// is a follow-up.
    /// </summary>
    private AlterUserScramCredentialsResponse HandleAlterUserScramCredentials(AlterUserScramCredentialsRequest request)
    {
        var results = new List<AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult>();

        foreach (var deletion in request.Deletions)
        {
            var (error, message) = TryRemoveScramUser(deletion.Name, deletion.Mechanism);
            results.Add(new AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
            {
                User = deletion.Name,
                ErrorCode = error,
                ErrorMessage = message,
            });
        }

        foreach (var upsert in request.Upsertions)
        {
            var (error, message) = TryUpsertScramUser(upsert);
            results.Add(new AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
            {
                User = upsert.Name,
                ErrorCode = error,
                ErrorMessage = message,
            });
        }

        return new AlterUserScramCredentialsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Results = results,
        };
    }

    private (ErrorCode error, string? message) TryRemoveScramUser(string user, sbyte mechanism)
    {
        var store = ResolveScramStore(mechanism);
        if (store is null) return UnknownMechanism(mechanism);
        if (string.IsNullOrEmpty(user)) return (ErrorCode.InvalidRequest, "User name must not be empty");

        if (store.RemoveUser(user))
        {
            _auditLogger?.LogAuthenticationEvent(
                AuditEventType.AuthorizationCheck,
                principal: user,
                clientAddress: null,
                mechanism: mechanism == MechanismScramSha256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512",
                success: true);
            return (ErrorCode.None, null);
        }
        return (ErrorCode.ResourceNotFound, "No credential exists for this (user, mechanism)");
    }

    private (ErrorCode error, string? message) TryUpsertScramUser(AlterUserScramCredentialsRequest.ScramCredentialUpsertion upsert)
    {
        var store = ResolveScramStore(upsert.Mechanism);
        if (store is null) return UnknownMechanism(upsert.Mechanism);
        if (string.IsNullOrEmpty(upsert.Name)) return (ErrorCode.InvalidRequest, "User name must not be empty");
        if (upsert.Iterations <= 0) return (ErrorCode.InvalidRequest, "Iterations must be positive");
        if (upsert.Salt is null || upsert.Salt.Length == 0) return (ErrorCode.InvalidRequest, "Salt must be supplied");
        if (upsert.SaltedPassword is null || upsert.SaltedPassword.Length == 0) return (ErrorCode.InvalidRequest, "SaltedPassword must be supplied");

        // The client already ran PBKDF2 to produce SaltedPassword. RFC 5802
        // says StoredKey = H(HMAC(SaltedPassword, "Client Key")) and
        // ServerKey = HMAC(SaltedPassword, "Server Key"). Surgewave's existing
        // store accepts the pre-computed shape via AddCredential — we just
        // derive the keys and hand them in. The HMAC / Hash variant
        // matches the per-store hash algorithm, which is fixed per store.
        var clientKey = HmacForMechanism(upsert.Mechanism, upsert.SaltedPassword, "Client Key");
        var serverKey = HmacForMechanism(upsert.Mechanism, upsert.SaltedPassword, "Server Key");
        var storedKey = HashForMechanism(upsert.Mechanism, clientKey);

        var credential = new ScramCredential
        {
            Username = upsert.Name,
            Salt = upsert.Salt,
            Iterations = upsert.Iterations,
            StoredKey = storedKey,
            ServerKey = serverKey,
        };
        store.AddCredential(credential);
        _auditLogger?.LogAuthenticationEvent(
            AuditEventType.AuthorizationCheck,
            principal: upsert.Name,
            clientAddress: null,
            mechanism: upsert.Mechanism == MechanismScramSha256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512",
            success: true);
        return (ErrorCode.None, null);
    }

    private ScramCredentialStore? ResolveScramStore(sbyte mechanism) => mechanism switch
    {
        MechanismScramSha256 => _scramSha256Store,
        MechanismScramSha512 => _scramSha512Store,
        _ => null,
    };

    private static (ErrorCode, string?) UnknownMechanism(sbyte mechanism) =>
        (ErrorCode.UnsupportedSaslMechanism, $"Unknown or unconfigured SCRAM mechanism code: {mechanism}");

    private static byte[] HmacForMechanism(sbyte mechanism, byte[] key, string data)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(data);
        return mechanism == MechanismScramSha512
            ? System.Security.Cryptography.HMACSHA512.HashData(key, bytes)
            : System.Security.Cryptography.HMACSHA256.HashData(key, bytes);
    }

    private static byte[] HashForMechanism(sbyte mechanism, byte[] data) =>
        mechanism == MechanismScramSha512
            ? System.Security.Cryptography.SHA512.HashData(data)
            : System.Security.Cryptography.SHA256.HashData(data);

    private IEnumerable<string> EnumerateAllStoredUsers()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        if (_scramSha256Store is not null)
        {
            foreach (var u in _scramSha256Store.ListUsers())
                if (seen.Add(u)) yield return u;
        }
        if (_scramSha512Store is not null)
        {
            foreach (var u in _scramSha512Store.ListUsers())
                if (seen.Add(u)) yield return u;
        }
    }

    private SaslHandshakeResponse HandleSaslHandshake(SaslHandshakeRequest request, ConnectionState connectionState)
    {
        if (_saslAuthenticator == null)
            return SaslHandshakeResponse.CreateSuccess(request.CorrelationId, request.ApiVersion, []);

        var enabledMechanisms = _saslAuthenticator.EnabledMechanisms;
        if (!_saslAuthenticator.IsMechanismSupported(request.Mechanism))
            return SaslHandshakeResponse.CreateError(request.CorrelationId, request.ApiVersion, ErrorCode.UnsupportedSaslMechanism, enabledMechanisms);

        connectionState.SetNegotiatedMechanism(request.Mechanism);
        return SaslHandshakeResponse.CreateSuccess(request.CorrelationId, request.ApiVersion, enabledMechanisms);
    }

    private SaslAuthenticateResponse HandleSaslAuthenticate(SaslAuthenticateRequest request, ConnectionState connectionState)
    {
        if (_saslAuthenticator == null)
            return SaslAuthenticateResponse.CreateSuccess(request.CorrelationId, request.ApiVersion);

        if (string.IsNullOrEmpty(connectionState.NegotiatedMechanism))
            return SaslAuthenticateResponse.CreateError(request.CorrelationId, request.ApiVersion, ErrorCode.IllegalSaslState, "SASL handshake must be performed before authentication");

        SaslAuthenticationResult result;
        if (_saslAuthenticator.IsMultiStepMechanism(connectionState.NegotiatedMechanism))
        {
            if (connectionState.ScramSession == null)
                return SaslAuthenticateResponse.CreateError(request.CorrelationId, request.ApiVersion, ErrorCode.IllegalSaslState, "SCRAM session not initialized");
            result = _saslAuthenticator.AuthenticateScram(connectionState.NegotiatedMechanism, request.AuthBytes, connectionState.ScramSession);
        }
        else
        {
            result = _saslAuthenticator.Authenticate(connectionState.NegotiatedMechanism, request.AuthBytes);
        }

        if (result.IsSuccess)
        {
            connectionState.SetAuthenticated(result.Username!, connectionState.NegotiatedMechanism);

            // Audit log successful authentication
            _auditLogger?.LogAuthenticationEvent(
                AuditEventType.AuthenticationSuccess,
                result.Username,
                connectionState.ClientHost,
                connectionState.NegotiatedMechanism,
                success: true);

            return SaslAuthenticateResponse.CreateSuccess(request.CorrelationId, request.ApiVersion, result.ResponseData);
        }

        if (!result.IsComplete)
            return SaslAuthenticateResponse.CreateChallenge(request.CorrelationId, request.ApiVersion, result.ResponseData!);

        // Audit log failed authentication
        _auditLogger?.LogAuthenticationEvent(
            AuditEventType.AuthenticationFailed,
            null,
            connectionState.ClientHost,
            connectionState.NegotiatedMechanism,
            success: false,
            errorMessage: result.ErrorMessage);

        return SaslAuthenticateResponse.CreateError(request.CorrelationId, request.ApiVersion, ErrorCode.SaslAuthenticationFailed, result.ErrorMessage);
    }

    private DescribeAclsResponse HandleDescribeAcls(DescribeAclsRequest request)
    {
        if (_aclAuthorizer == null)
            return new DescribeAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, ErrorCode = ErrorCode.SecurityDisabled, ErrorMessage = "ACL authorization is not enabled", Resources = [] };

        var matchingAcls = _aclAuthorizer.ListAcls(acl =>
        {
            if (request.ResourceTypeFilter != AclResourceTypeFilter.Any && (int)request.ResourceTypeFilter != (int)acl.ResourceType) return false;
            if (request.ResourceNameFilter != null && !string.Equals(request.ResourceNameFilter, acl.ResourceName, StringComparison.OrdinalIgnoreCase)) return false;
            if (request.PatternTypeFilter != AclPatternTypeFilter.Any && request.PatternTypeFilter != AclPatternTypeFilter.Match && (int)request.PatternTypeFilter != (int)acl.PatternType) return false;
            if (request.PrincipalFilter != null && !string.Equals(request.PrincipalFilter, acl.Principal, StringComparison.OrdinalIgnoreCase)) return false;
            if (request.HostFilter != null && !string.Equals(request.HostFilter, acl.Host, StringComparison.OrdinalIgnoreCase)) return false;
            if (request.OperationFilter != AclOperationFilter.Any && (int)request.OperationFilter != (int)acl.Operation) return false;
            if (request.PermissionTypeFilter != AclPermissionTypeFilter.Any && (int)request.PermissionTypeFilter != (int)acl.Permission) return false;
            return true;
        });

        var resourceGroups = matchingAcls.GroupBy(acl => (acl.ResourceType, acl.ResourceName, acl.PatternType))
            .Select(g => new DescribeAclsResponse.AclResource
            {
                ResourceType = (AclResourceTypeFilter)(int)g.Key.ResourceType,
                ResourceName = g.Key.ResourceName,
                PatternType = (AclPatternTypeFilter)(int)g.Key.PatternType,
                Acls = g.Select(acl => new DescribeAclsResponse.AclBinding { Principal = acl.Principal, Host = acl.Host, Operation = (AclOperationFilter)(int)acl.Operation, PermissionType = (AclPermissionTypeFilter)(int)acl.Permission }).ToList()
            }).ToList();

        return new DescribeAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, ErrorCode = ErrorCode.None, Resources = resourceGroups };
    }

    private CreateAclsResponse HandleCreateAcls(CreateAclsRequest request, RequestContext context)
    {
        var results = new List<CreateAclsResponse.AclCreationResult>();

        if (_aclAuthorizer == null)
        {
            foreach (var _ in request.Creations)
                results.Add(new CreateAclsResponse.AclCreationResult { ErrorCode = ErrorCode.SecurityDisabled, ErrorMessage = "ACL authorization is not enabled" });
            return new CreateAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, Results = results };
        }

        foreach (var creation in request.Creations)
        {
            try
            {
                _aclAuthorizer.AddAcl(new AclEntry
                {
                    Principal = creation.Principal, Host = creation.Host,
                    ResourceType = (AclResourceType)(int)creation.ResourceType, ResourceName = creation.ResourceName,
                    PatternType = (AclPatternType)(int)creation.PatternType,
                    Operation = (AclOperation)(int)creation.Operation, Permission = (AclPermission)(int)creation.PermissionType
                });

                // Audit log the ACL creation
                _auditLogger?.LogAclEvent(
                    AuditEventType.AclCreated,
                    creation.ResourceType.ToString(),
                    creation.ResourceName,
                    context.ConnectionState.AuthenticatedUser,
                    context.ConnectionState.ClientHost,
                    success: true,
                    details: new Dictionary<string, string>
                    {
                        ["principal"] = creation.Principal,
                        ["operation"] = creation.Operation.ToString(),
                        ["permission"] = creation.PermissionType.ToString()
                    });

                results.Add(new CreateAclsResponse.AclCreationResult { ErrorCode = ErrorCode.None });
            }
            catch (Exception ex)
            {
                results.Add(new CreateAclsResponse.AclCreationResult { ErrorCode = ErrorCode.Unknown, ErrorMessage = ex.Message });
            }
        }

        if (_config.AclFile != null)
        {
            try { _aclAuthorizer.SaveToFile(_config.AclFile); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to save ACLs to file"); }
        }

        return new CreateAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, Results = results };
    }

    private DeleteAclsResponse HandleDeleteAcls(DeleteAclsRequest request, RequestContext context)
    {
        var filterResults = new List<DeleteAclsResponse.AclFilterResult>();

        if (_aclAuthorizer == null)
        {
            foreach (var _ in request.Filters)
                filterResults.Add(new DeleteAclsResponse.AclFilterResult { ErrorCode = ErrorCode.SecurityDisabled, ErrorMessage = "ACL authorization is not enabled", MatchingAcls = [] });
            return new DeleteAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, FilterResults = filterResults };
        }

        foreach (var filter in request.Filters)
        {
            try
            {
                var matchingAcls = _aclAuthorizer.ListAcls(acl =>
                {
                    if (filter.ResourceTypeFilter != AclResourceTypeFilter.Any && (int)filter.ResourceTypeFilter != (int)acl.ResourceType) return false;
                    if (filter.ResourceNameFilter != null && !string.Equals(filter.ResourceNameFilter, acl.ResourceName, StringComparison.OrdinalIgnoreCase)) return false;
                    if (filter.PatternTypeFilter != AclPatternTypeFilter.Any && filter.PatternTypeFilter != AclPatternTypeFilter.Match && (int)filter.PatternTypeFilter != (int)acl.PatternType) return false;
                    if (filter.PrincipalFilter != null && !string.Equals(filter.PrincipalFilter, acl.Principal, StringComparison.OrdinalIgnoreCase)) return false;
                    if (filter.HostFilter != null && !string.Equals(filter.HostFilter, acl.Host, StringComparison.OrdinalIgnoreCase)) return false;
                    if (filter.OperationFilter != AclOperationFilter.Any && (int)filter.OperationFilter != (int)acl.Operation) return false;
                    if (filter.PermissionTypeFilter != AclPermissionTypeFilter.Any && (int)filter.PermissionTypeFilter != (int)acl.Permission) return false;
                    return true;
                }).ToList();

                var matchingAclResponses = matchingAcls.Select(acl => new DeleteAclsResponse.MatchingAcl
                {
                    ErrorCode = ErrorCode.None, ResourceType = (AclResourceTypeFilter)(int)acl.ResourceType, ResourceName = acl.ResourceName,
                    PatternType = (AclPatternTypeFilter)(int)acl.PatternType, Principal = acl.Principal, Host = acl.Host,
                    Operation = (AclOperationFilter)(int)acl.Operation, PermissionType = (AclPermissionTypeFilter)(int)acl.Permission
                }).ToList();

                _aclAuthorizer.RemoveAcls(acl => matchingAcls.Contains(acl));

                // Audit log the ACL deletions
                foreach (var acl in matchingAcls)
                {
                    _auditLogger?.LogAclEvent(
                        AuditEventType.AclDeleted,
                        acl.ResourceType.ToString(),
                        acl.ResourceName,
                        context.ConnectionState.AuthenticatedUser,
                        context.ConnectionState.ClientHost,
                        success: true,
                        details: new Dictionary<string, string>
                        {
                            ["principal"] = acl.Principal,
                            ["operation"] = acl.Operation.ToString(),
                            ["permission"] = acl.Permission.ToString()
                        });
                }

                filterResults.Add(new DeleteAclsResponse.AclFilterResult { ErrorCode = ErrorCode.None, MatchingAcls = matchingAclResponses });
            }
            catch (Exception ex)
            {
                filterResults.Add(new DeleteAclsResponse.AclFilterResult { ErrorCode = ErrorCode.Unknown, ErrorMessage = ex.Message, MatchingAcls = [] });
            }
        }

        if (_config.AclFile != null)
        {
            try { _aclAuthorizer.SaveToFile(_config.AclFile); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to save ACLs to file"); }
        }

        return new DeleteAclsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = 0, FilterResults = filterResults };
    }
}
