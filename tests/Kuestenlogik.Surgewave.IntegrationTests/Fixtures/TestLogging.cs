using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// The console logger the broker fixtures run on.
/// </summary>
/// <remarks>
/// Stated in code on purpose: a test project is its own composition root, so there is no operator
/// to reconfigure it and nothing a settings file would add. What matters is only that the level is
/// stated ONCE rather than per fixture, which is how this suite drifted — the shared
/// <see cref="BrokerFixture"/> sat at Debug while every other fixture sat at Warning, and logged
/// every broker's Debug output to the console for a whole CI run. Several gigabytes per run, on a
/// green run as much as a red one, leaving a log too large to open on the day there is something
/// in it worth finding.
/// <para>
/// Warning, because the console is process-wide: it knows no test boundaries and lands in the CI
/// job log whether or not anything failed. A test that wants detail for its own diagnosis takes an
/// <c>ITestOutputHelper</c> sink instead, which xunit buffers per test and prints only on failure.
/// </para>
/// </remarks>
internal static class TestLogging
{
    public const LogLevel ConsoleLevel = LogLevel.Warning;

    public static ILoggerFactory CreateConsoleFactory()
        => LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(ConsoleLevel);
            builder.AddConsole();
        });
}
