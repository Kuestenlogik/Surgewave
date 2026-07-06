using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// The single registered <see cref="IClaimsTransformation"/>: it first runs the
/// provider-routing <see cref="CompositeClaimsTransformation"/> (IdP-sourced
/// role claims), then augments the principal with role claims derived from the
/// server-side role store. This is what makes RoleManagement assignments
/// actually enforced — the added claims flow straight into the existing
/// RequireRole policies with no change to <see cref="SurgewavePolicies"/>.
/// </summary>
public sealed class StoreRoleClaimsTransformation(
    CompositeClaimsTransformation inner,
    RoleClaimAugmenter augmenter) : IClaimsTransformation
{
    public async Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
    {
        principal = await inner.TransformAsync(principal);
        augmenter.Augment(principal);
        return principal;
    }
}
