using System.Security.Claims;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Pins down the broker-side OAUTHBEARER wiring (KIP-936): the SASL authenticator
/// must accept OAUTHBEARER as a single-step mechanism only when a validator was
/// passed at construction time, and must reject it cleanly when the operator
/// listed the mechanism in config but never wired the validator.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SaslOAuthBearerWiringTests
{
    private const char Soh = '\x01';

    [Fact]
    public void EnabledMechanisms_IncludesOauthBearer_WhenListedAndValidatorWired()
    {
        var authenticator = BuildAuthenticator(
            mechanisms: ["OAUTHBEARER"],
            validatorOutcome: ValidatorOutcome.Succeed);

        Assert.Contains("OAUTHBEARER", authenticator.EnabledMechanisms,
            StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void IsMultiStepMechanism_OauthBearer_IsSingleStep()
    {
        var authenticator = BuildAuthenticator(
            mechanisms: ["OAUTHBEARER"],
            validatorOutcome: ValidatorOutcome.Succeed);

        Assert.False(authenticator.IsMultiStepMechanism("OAUTHBEARER"));
    }

    [Fact]
    public void Authenticate_OauthBearer_WithValidator_ReturnsPrincipal()
    {
        var authenticator = BuildAuthenticator(
            mechanisms: ["OAUTHBEARER"],
            validatorOutcome: ValidatorOutcome.Succeed);

        var frame = OauthBearerFrame("good-token");
        var result = authenticator.Authenticate("OAUTHBEARER", frame);

        Assert.True(result.IsSuccess);
        Assert.Equal("alice@example.com", result.Username);
    }

    [Fact]
    public void Authenticate_OauthBearer_WithoutValidator_FailsCleanly()
    {
        // Operator listed OAUTHBEARER but never wired a validator — the
        // authenticator must report a precise reason rather than throw.
        var authenticator = new SaslAuthenticator(
            credentialStore: new CredentialStore(),
            enabledMechanisms: ["OAUTHBEARER"],
            scramSha256Store: null,
            scramSha512Store: null,
            oauthBearer: null);

        var result = authenticator.Authenticate("OAUTHBEARER", OauthBearerFrame("any"));

        Assert.False(result.IsSuccess);
        Assert.Equal("OAUTHBEARER not configured", result.ErrorMessage);
    }

    [Fact]
    public void Authenticate_OauthBearer_InvalidToken_FailsWithoutLeakingValidatorReason()
    {
        var authenticator = BuildAuthenticator(
            mechanisms: ["OAUTHBEARER"],
            validatorOutcome: ValidatorOutcome.Fail);

        var result = authenticator.Authenticate("OAUTHBEARER", OauthBearerFrame("bad-token"));

        Assert.False(result.IsSuccess);
        // Wire-facing message is the generic envelope — the validator's internal
        // reason ("stub failure") must not surface to clients.
        Assert.Equal("Invalid OAUTHBEARER token", result.ErrorMessage);
    }

    [Fact]
    public void Authenticate_OauthBearer_NotInMechanismList_RejectedByMechanismGate()
    {
        // The operator only allowed PLAIN; OAUTHBEARER must be refused even if
        // the validator was wired up.
        var authenticator = BuildAuthenticator(
            mechanisms: ["PLAIN"],
            validatorOutcome: ValidatorOutcome.Succeed);

        var result = authenticator.Authenticate("OAUTHBEARER", OauthBearerFrame("good"));

        Assert.False(result.IsSuccess);
        Assert.Equal("Unsupported SASL mechanism", result.ErrorMessage);
    }

    private static SaslAuthenticator BuildAuthenticator(
        string[] mechanisms,
        ValidatorOutcome validatorOutcome)
    {
        var validator = new StubValidator(validatorOutcome);
        var oauth = new OAuthBearerAuthenticator(
            validator,
            new OAuthBearerConfig { Enabled = true, PrincipalClaim = "sub" });

        return new SaslAuthenticator(
            credentialStore: new CredentialStore(),
            enabledMechanisms: mechanisms,
            scramSha256Store: null,
            scramSha512Store: null,
            oauthBearer: oauth);
    }

    private static byte[] OauthBearerFrame(string token) =>
        Encoding.UTF8.GetBytes($"n,,{Soh}auth=Bearer {token}{Soh}{Soh}");

    private enum ValidatorOutcome { Succeed, Fail }

    private sealed class StubValidator : IOAuthBearerTokenValidator
    {
        private readonly ValidatorOutcome _outcome;
        public StubValidator(ValidatorOutcome outcome) => _outcome = outcome;

        public Task<OAuthBearerValidationResult> ValidateAsync(string token, CancellationToken cancellationToken)
        {
            if (_outcome == ValidatorOutcome.Fail)
                return Task.FromResult(OAuthBearerValidationResult.Failure("stub failure"));

            var identity = new ClaimsIdentity("test");
            identity.AddClaim(new Claim("sub", "alice@example.com"));
            return Task.FromResult(OAuthBearerValidationResult.Success(
                new ClaimsPrincipal(identity),
                DateTimeOffset.UtcNow.AddHours(1)));
        }
    }
}
