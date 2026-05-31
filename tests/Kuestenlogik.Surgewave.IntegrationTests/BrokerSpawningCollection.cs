using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Seriellisierter Bucket fuer IntegrationTests-Files, die jeweils einen
/// eigenen in-process SurgewaveRuntime hochziehen — IPv4BindingTests,
/// IsolatedPerformanceTests, KafkaProtocolApiCoverageTests, ReplicationTests.
/// Ohne Collection-Marker wuerde xunit alle zugehoerigen [Fact]-Methoden
/// parallel laufen lassen → jeder einen eigenen broker → Port- und
/// Prozess-Exhaustion auf dem GitHub-Actions Linux-Runner (gleiches
/// Root-Cause wie ExactlyOnceCollection in Connect.Eos.Tests).
///
/// Die Tests in [Collection("Broker")] teilen sich dagegen einen einzelnen
/// Fixture-Broker und kollidieren nicht; sie bleiben deshalb in ihrer
/// eigenen Collection.
/// </summary>
[CollectionDefinition(nameof(BrokerSpawningCollection), DisableParallelization = true)]
public sealed class BrokerSpawningCollection { }
