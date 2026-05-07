using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Helpers;

/// <summary>
/// Base class for integration tests that require a running broker.
/// </summary>
[Collection("Broker")]
public abstract class IntegrationTestBase
{
    private readonly BrokerFixture _fixture;
    private readonly ITestOutputHelper _output;

    protected IntegrationTestBase(BrokerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    protected BrokerFixture Fixture => _fixture;
    protected ITestOutputHelper Output => _output;

    /// <summary>
    /// Gets the bootstrap servers address for connecting to the test broker.
    /// </summary>
    protected string BootstrapServers => BrokerFixture.BootstrapServers;
}
