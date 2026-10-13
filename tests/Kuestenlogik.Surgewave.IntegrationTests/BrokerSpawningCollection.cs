using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Serialised bucket for IntegrationTests files that each spin up their
/// own in-process SurgewaveRuntime — IPv4BindingTests,
/// IsolatedPerformanceTests, KafkaProtocolApiCoverageTests, ReplicationTests.
/// Without the collection marker xunit would run all their [Fact] methods
/// in parallel → each its own broker → port and
/// process exhaustion on the GitHub Actions Linux runner (the same
/// root cause as ExactlyOnceCollection in Connect.Eos.Tests).
///
/// The tests in [Collection("Broker")], by contrast, share a single
/// fixture broker and do not collide; they therefore stay in their
/// own collection.
/// </summary>
[CollectionDefinition(nameof(BrokerSpawningCollection), DisableParallelization = true)]
public sealed class BrokerSpawningCollection { }
