using Xunit;

namespace Kuestenlogik.Surgewave.Tests.Helpers;

/// <summary>
/// Base class for all Surgewave tests providing common utilities and logging.
/// </summary>
public abstract class SurgewaveTestBase
{
    private readonly ITestOutputHelper _output;

    protected ITestOutputHelper Output => _output;

    protected SurgewaveTestBase(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// Logs a message to the test output.
    /// </summary>
    protected void Log(string message)
    {
        Output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {message}");
    }

    /// <summary>
    /// Logs a formatted message to the test output.
    /// </summary>
    protected void Log(string format, params object[] args)
    {
        Output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {string.Format(format, args)}");
    }

    /// <summary>
    /// Generates a unique topic name for this test.
    /// </summary>
    protected string GenerateTopicName(string prefix = "test")
    {
        return $"{prefix}-{Guid.NewGuid():N}";
    }

    /// <summary>
    /// Generates a unique group ID for this test.
    /// </summary>
    protected string GenerateGroupId(string prefix = "test-group")
    {
        return $"{prefix}-{Guid.NewGuid():N}";
    }
}
