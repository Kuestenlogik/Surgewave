using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Helper for tests that exercise loopback networking. CI runners (GitHub
/// Actions, in particular) regularly drop loopback UDP packets and stall
/// loopback TCP handshakes under load, which makes statistical / timing
/// proxy tests flaky on CI even though the proxy code is correct. We
/// skip those tests on CI; they still run locally.
/// </summary>
internal static class CiSkip
{
    public static void IfRunningOnCi(string reason)
    {
        if (string.Equals(Environment.GetEnvironmentVariable("CI"), "true",
                StringComparison.OrdinalIgnoreCase) ||
            string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true",
                StringComparison.OrdinalIgnoreCase))
        {
            Assert.Skip(reason);
        }
    }
}
