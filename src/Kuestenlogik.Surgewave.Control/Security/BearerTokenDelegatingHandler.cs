using System.Net.Http.Headers;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Propagates the user's OIDC access token as Bearer header to Broker API calls.
/// </summary>
public sealed class BearerTokenDelegatingHandler(IHttpContextAccessor httpContextAccessor) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var httpContext = httpContextAccessor.HttpContext;
        if (httpContext is not null)
        {
            var accessToken = await httpContext.GetTokenAsync("access_token");
            if (!string.IsNullOrEmpty(accessToken))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            }
        }

        return await base.SendAsync(request, cancellationToken);
    }
}
