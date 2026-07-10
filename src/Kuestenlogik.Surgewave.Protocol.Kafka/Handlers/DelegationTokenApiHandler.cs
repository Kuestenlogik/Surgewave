using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for Kafka delegation token APIs (API Keys 38-41).
/// </summary>
public sealed partial class DelegationTokenApiHandler : IKafkaRequestHandler
{
    private readonly IDelegationTokenService _tokenManager;
    private readonly ILogger<DelegationTokenApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.CreateDelegationToken,
        ApiKey.RenewDelegationToken,
        ApiKey.ExpireDelegationToken,
        ApiKey.DescribeDelegationToken
    ];

    public DelegationTokenApiHandler(IDelegationTokenService tokenManager, ILogger<DelegationTokenApiHandler> logger)
    {
        _tokenManager = tokenManager;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult<KafkaResponse>(request switch
        {
            CreateDelegationTokenRequest createRequest => HandleCreateDelegationToken(createRequest, context),
            RenewDelegationTokenRequest renewRequest => HandleRenewDelegationToken(renewRequest),
            ExpireDelegationTokenRequest expireRequest => HandleExpireDelegationToken(expireRequest),
            DescribeDelegationTokenRequest describeRequest => HandleDescribeDelegationToken(describeRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by DelegationTokenApiHandler")
        });
    }

    private CreateDelegationTokenResponse HandleCreateDelegationToken(CreateDelegationTokenRequest request, RequestContext context)
    {
        if (!_tokenManager.Config.Enabled)
        {
            return new CreateDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenAuthDisabled,
                PrincipalType = "",
                PrincipalName = "",
                TokenId = "",
                Hmac = [],
                ThrottleTimeMs = 0
            };
        }

        // Determine owner - use request owner or authenticated user
        var ownerPrincipalType = request.OwnerPrincipalType ?? "User";
        var ownerPrincipalName = request.OwnerPrincipalName ??
            (context.ConnectionState.IsAuthenticated ? context.ConnectionState.AuthenticatedUser : "anonymous");

        // Requester is the authenticated user making the request
        var requesterPrincipalType = "User";
        var requesterPrincipalName = context.ConnectionState.IsAuthenticated
            ? context.ConnectionState.AuthenticatedUser
            : "anonymous";

        // Convert renewers
        var renewers = request.Renewers?.Select(r => new TokenRenewer
        {
            PrincipalType = r.PrincipalType,
            PrincipalName = r.PrincipalName
        }).ToList();

        try
        {
            var token = _tokenManager.CreateToken(
                ownerPrincipalType,
                ownerPrincipalName!,
                requesterPrincipalType,
                requesterPrincipalName,
                renewers,
                request.MaxLifetimeMs);

            LogCreateToken(token.TokenId, $"{ownerPrincipalType}:{ownerPrincipalName}");

            return new CreateDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                PrincipalType = token.OwnerPrincipalType,
                PrincipalName = token.OwnerPrincipalName,
                TokenRequesterPrincipalType = token.RequesterPrincipalType,
                TokenRequesterPrincipalName = token.RequesterPrincipalName,
                IssueTimestampMs = token.IssueTimestampMs,
                ExpiryTimestampMs = token.ExpiryTimestampMs,
                MaxTimestampMs = token.MaxTimestampMs,
                TokenId = token.TokenId,
                Hmac = token.Hmac,
                ThrottleTimeMs = 0
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create delegation token");
            return new CreateDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenRequestNotAllowed,
                PrincipalType = ownerPrincipalType,
                PrincipalName = ownerPrincipalName!,
                TokenId = "",
                Hmac = [],
                ThrottleTimeMs = 0
            };
        }
    }

    private RenewDelegationTokenResponse HandleRenewDelegationToken(RenewDelegationTokenRequest request)
    {
        if (!_tokenManager.Config.Enabled)
        {
            return new RenewDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenAuthDisabled,
                ExpiryTimestampMs = -1,
                ThrottleTimeMs = 0
            };
        }

        var (token, error) = _tokenManager.RenewToken(request.Hmac, request.RenewPeriodMs);

        if (error != null)
        {
            return new RenewDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = error == "Token not found" ? ErrorCode.DelegationTokenNotFound : ErrorCode.DelegationTokenExpired,
                ExpiryTimestampMs = -1,
                ThrottleTimeMs = 0
            };
        }

        LogRenewToken(token!.TokenId);

        return new RenewDelegationTokenResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            ExpiryTimestampMs = token.ExpiryTimestampMs,
            ThrottleTimeMs = 0
        };
    }

    private ExpireDelegationTokenResponse HandleExpireDelegationToken(ExpireDelegationTokenRequest request)
    {
        if (!_tokenManager.Config.Enabled)
        {
            return new ExpireDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenAuthDisabled,
                ExpiryTimestampMs = -1,
                ThrottleTimeMs = 0
            };
        }

        var (expiryTimestamp, error) = _tokenManager.ExpireToken(request.Hmac, request.ExpiryTimePeriodMs);

        if (error != null)
        {
            return new ExpireDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenNotFound,
                ExpiryTimestampMs = -1,
                ThrottleTimeMs = 0
            };
        }

        LogExpireToken(expiryTimestamp);

        return new ExpireDelegationTokenResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            ExpiryTimestampMs = expiryTimestamp,
            ThrottleTimeMs = 0
        };
    }

    private DescribeDelegationTokenResponse HandleDescribeDelegationToken(DescribeDelegationTokenRequest request)
    {
        if (!_tokenManager.Config.Enabled)
        {
            return new DescribeDelegationTokenResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.DelegationTokenAuthDisabled,
                Tokens = [],
                ThrottleTimeMs = 0
            };
        }

        // Convert owner filter
        var owners = request.Owners?.Select(o => new TokenOwner
        {
            PrincipalType = o.PrincipalType,
            PrincipalName = o.PrincipalName
        }).ToList();

        var tokens = _tokenManager.DescribeTokens(owners);

        LogDescribeTokens(tokens.Count);

        var responseTokens = tokens.Select(t => new DescribeDelegationTokenResponse.DescribedDelegationToken
        {
            PrincipalType = t.OwnerPrincipalType,
            PrincipalName = t.OwnerPrincipalName,
            TokenRequesterPrincipalType = t.RequesterPrincipalType,
            TokenRequesterPrincipalName = t.RequesterPrincipalName,
            IssueTimestampMs = t.IssueTimestampMs,
            ExpiryTimestampMs = t.ExpiryTimestampMs,
            MaxTimestampMs = t.MaxTimestampMs,
            TokenId = t.TokenId,
            Hmac = t.Hmac,
            Renewers = t.Renewers.Select(r => new DescribeDelegationTokenResponse.DescribedDelegationTokenRenewer
            {
                PrincipalType = r.PrincipalType,
                PrincipalName = r.PrincipalName
            }).ToList()
        }).ToList();

        return new DescribeDelegationTokenResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Tokens = responseTokens,
            ThrottleTimeMs = 0
        };
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created delegation token {TokenId} for {Owner}")]
    private partial void LogCreateToken(string tokenId, string owner);

    [LoggerMessage(Level = LogLevel.Information, Message = "Renewed delegation token {TokenId}")]
    private partial void LogRenewToken(string tokenId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Set expiry for delegation token to {ExpiryTimestamp}")]
    private partial void LogExpireToken(long expiryTimestamp);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Described {Count} delegation tokens")]
    private partial void LogDescribeTokens(int count);
}
