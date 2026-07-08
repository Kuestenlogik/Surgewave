using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-664 DescribeProducers (API key 61) — pin the projection from
/// <see cref="ProducerStateManager"/> internal state to the on-the-wire
/// <see cref="DescribeProducersResponse"/>. The classic invariant: a
/// producer that has never written to or held a transaction on a partition
/// must NOT show up in that partition's listing — otherwise admin tools
/// see false-positive "active producers" everywhere.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeProducersTests
{
    [Fact]
    public void GetActiveProducersForPartition_NoActivity_ReturnsEmpty()
    {
        var psm = new ProducerStateManager();
        var (pid, epoch) = psm.AllocateProducerId();

        // Producer exists in the manager but never touched any partition.
        var orders = new TopicPartition { Topic = "orders", Partition = 0 };
        var result = psm.GetActiveProducersForPartition(orders);

        Assert.Empty(result);
        // Sanity — the producer is allocated, just inactive on this partition.
        Assert.True(pid > 0);
        Assert.Equal((short)0, epoch);
    }

    [Fact]
    public void GetActiveProducersForPartition_AfterValidateSequence_ListsProducer()
    {
        var psm = new ProducerStateManager();
        var (pid, epoch) = psm.AllocateProducerId();
        var orders = new TopicPartition { Topic = "orders", Partition = 0 };

        // ValidateSequence updates the per-partition last-seq state on success.
        var result = psm.ValidateSequence(pid, epoch, baseSequence: 0, orders);
        Assert.Equal(ErrorCode.None, result);

        var producers = psm.GetActiveProducersForPartition(orders);

        var entry = Assert.Single(producers);
        Assert.Equal(pid, entry.ProducerId);
        Assert.Equal(epoch, entry.Epoch);
        Assert.False(entry.HasOngoingTransaction);
        // First batch sequence ends at 0 — librdkafka starts at 0 and
        // increments per record. Surgewave tracks the LAST written sequence.
        Assert.Equal(0, entry.LastSequence);
    }

    [Fact]
    public void GetActiveProducersForPartition_OnlyOnTransactionPartitions_ListsProducer()
    {
        var psm = new ProducerStateManager();
        var (pid, epoch) = psm.AllocateProducerId();
        psm.RegisterProducerId(pid, epoch);
        psm.BeginTransaction(pid, epoch);

        var partition = new TopicPartition { Topic = "txn", Partition = 0 };
        psm.AddPartitionToTransaction(pid, epoch, partition);

        var producers = psm.GetActiveProducersForPartition(partition);

        var entry = Assert.Single(producers);
        Assert.Equal(pid, entry.ProducerId);
        Assert.True(entry.HasOngoingTransaction);
    }

    [Fact]
    public void GetActiveProducersForPartition_PerPartitionIsolation()
    {
        // Producer A writes to orders-0 only; producer B writes to payments-0
        // only. Each partition's listing must contain exactly one producer.
        var psm = new ProducerStateManager();
        var (pidA, epA) = psm.AllocateProducerId();
        var (pidB, epB) = psm.AllocateProducerId();
        var orders = new TopicPartition { Topic = "orders", Partition = 0 };
        var payments = new TopicPartition { Topic = "payments", Partition = 0 };

        psm.ValidateSequence(pidA, epA, baseSequence: 0, orders);
        psm.ValidateSequence(pidB, epB, baseSequence: 0, payments);

        var ordersProducers = psm.GetActiveProducersForPartition(orders);
        var paymentsProducers = psm.GetActiveProducersForPartition(payments);

        Assert.Equal(pidA, Assert.Single(ordersProducers).ProducerId);
        Assert.Equal(pidB, Assert.Single(paymentsProducers).ProducerId);
    }

    [Fact]
    public async Task HandleDescribeProducers_RouteThroughTransactionCoordinator()
    {
        // Build a coordinator + producer state and assert the wire-shape
        // response matches the per-partition projection.
        var (coordinator, psm, _) = TestFixture.Build();
        var (pid, epoch) = psm.AllocateProducerId();
        var orders = new TopicPartition { Topic = "orders", Partition = 1 };
        psm.ValidateSequence(pid, epoch, baseSequence: 5, orders);

        // The DTO wire-shape mapping lives in the adapter now (#59), so drive the request through it.
        var handler = new TransactionApiHandler(coordinator, NullLogger<TransactionApiHandler>.Instance);
        var resp = (DescribeProducersResponse)await handler.HandleAsync(new DescribeProducersRequest
        {
            ApiKey = ApiKey.DescribeProducers,
            ApiVersion = 0,
            CorrelationId = 7,
            ClientId = "admin",
            Topics =
            [
                new DescribeProducersRequest.TopicRequest
                {
                    Name = "orders",
                    PartitionIndexes = [0, 1, 2],
                },
            ],
        }, new RequestContext { ConnectionState = new ConnectionState("describe-producers-test"), ClientId = "admin" }, CancellationToken.None);

        Assert.Equal(7, resp.CorrelationId);
        var topic = Assert.Single(resp.Topics);
        Assert.Equal("orders", topic.Name);
        Assert.Equal(3, topic.Partitions.Count);

        var p0 = topic.Partitions.Single(p => p.PartitionIndex == 0);
        var p1 = topic.Partitions.Single(p => p.PartitionIndex == 1);
        var p2 = topic.Partitions.Single(p => p.PartitionIndex == 2);

        Assert.Empty(p0.ActiveProducers);
        var listed = Assert.Single(p1.ActiveProducers);
        Assert.Equal(pid, listed.ProducerId);
        Assert.Equal(5, listed.LastSequence);
        Assert.Empty(p2.ActiveProducers);

        await coordinator.DisposeAsync();
    }

    private static class TestFixture
    {
        public static (TransactionCoordinator coord, ProducerStateManager psm, IDisposable scope) Build()
        {
            var temp = Path.Combine(Path.GetTempPath(), "surgewave-describe-producers-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(temp);

            var psm = new ProducerStateManager();
            var idx = new TransactionIndex();
            var logManager = TestLogManager.CreateInMemory(temp);
            var offsetStore = new OffsetStore(temp, Microsoft.Extensions.Logging.Abstractions.NullLogger<OffsetStore>.Instance);
            var stateStore = new TransactionStateStore(temp, Microsoft.Extensions.Logging.Abstractions.NullLogger<TransactionStateStore>.Instance);
            var coord = new TransactionCoordinator(psm, logManager, idx, offsetStore, stateStore,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<TransactionCoordinator>.Instance);

            var scope = new TempScope(temp, logManager, offsetStore, stateStore);
            return (coord, psm, scope);
        }

        private sealed class TempScope : IDisposable
        {
            private readonly string _dir;
            private readonly IDisposable[] _disposables;
            public TempScope(string dir, params IDisposable[] disposables)
            {
                _dir = dir;
                _disposables = disposables;
            }
            public void Dispose()
            {
                foreach (var d in _disposables)
                {
                    try { d.Dispose(); } catch { /* best effort */ }
                }
                try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
            }
        }
    }
}
