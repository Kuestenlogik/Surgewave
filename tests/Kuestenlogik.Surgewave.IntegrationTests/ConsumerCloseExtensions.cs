using System.Diagnostics;
using System.Reflection;
using Confluent.Kafka;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Closes a Confluent consumer so that a close that does not return fails the test instead of hanging the run.
/// </summary>
/// <remarks>
/// <para>
/// <c>IConsumer.Close()</c> takes neither a timeout nor a token. It returns when librdkafka's
/// <c>consumer_close</c> has served the consumer's last rebalance callback, committed its offsets and left the
/// group, and it runs that callback on the calling thread.
/// </para>
/// <para>
/// It does not always return. A rebalance event the application has not served yet when it calls <c>Close()</c> —
/// partitions assigned or revoked while the consumer was not polled — is served from inside
/// <c>consumer_close</c>, but the group no longer waits for it: it leaves and terminates as soon as the
/// coordinator has answered. The callback of Confluent.Kafka asks the group for its rebalance protocol and then
/// assigns or unassigns, each a request to the group that waits for its answer without a timeout, and librdkafka
/// (2.14.0, which these tests run on; 2.16.0 reads the same) destroys a request that reaches a terminated group
/// without answering it. A request sent in that window is never answered and <c>Close()</c> never returns, with
/// the broker having answered everything it was asked.
/// </para>
/// <para>
/// xunit's <c>Timeout</c> cannot end such a call: it ends a test that has yielded, not a thread blocked in native
/// code. So the close runs on a thread of its own and the test waits for it to finish, for at most
/// <see cref="Bound"/> or until the test is cancelled, and fails saying what it waited for. The thread that is
/// still in <c>consumer_close</c> is a background thread and ends with the process.
/// </para>
/// <para>
/// A consumer whose close did not return is not disposed either: <c>Dispose()</c> destroys the client under the
/// close that is still running and waits for it, so the test would hang in the <c>using</c> statement that follows
/// instead (2 of 12 closes that did not return; the other ten were disposed without a wait). Such a consumer is
/// marked as disposed, which is what a <c>Close()</c> that returns does to it, so the <c>Dispose()</c> that
/// follows does nothing; the client stays behind until the process ends.
/// </para>
/// </remarks>
internal static class ConsumerCloseExtensions
{
    /// <summary>
    /// How long a close may take: librdkafka's default <c>socket.timeout.ms</c>, the longest a request of the
    /// close — the offset commit, the LeaveGroup — stays unanswered before the client itself gives it up.
    /// </summary>
    public static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    // What Confluent.Kafka's Consumer.Dispose(bool) checks before it destroys the client, and sets itself.
    private const string DisposedField = "disposeHasBeenCalled";

    /// <summary>
    /// Calls <see cref="IConsumer{TKey,TValue}.Close"/> and fails the test if it has not returned within
    /// <see cref="Bound"/> or by the time the test is cancelled. What <c>Close()</c> throws is thrown as it is.
    /// </summary>
    public static void CloseWithinBound<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var name = consumer.Name;
        var started = Stopwatch.GetTimestamp();
        var close = Task.Factory.StartNew(consumer.Close, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // The done signal of the close, the bound or the test's cancellation, whichever comes first.
        bool returned;
        try
        {
            returned = close.Wait(Bound, cancellationToken);
        }
        catch (AggregateException)
        {
            // Close() threw: it has returned, and its exception is rethrown below.
            returned = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            returned = false;
        }

        if (!returned)
        {
            var markedDisposed = MarkDisposed(consumer);
            Assert.Fail(
                $"Close() of consumer '{name}' has not returned after {Stopwatch.GetElapsedTime(started).TotalSeconds:F1} s " +
                (cancellationToken.IsCancellationRequested ? "when the test was cancelled" : $"(bound {Bound.TotalSeconds:F0} s)") +
                ": librdkafka's consumer_close is still waiting — for the coordinator's answer to the offset commit or the " +
                "LeaveGroup, or for the answer to a request the consumer's last rebalance callback sent to a consumer " +
                "group that had already terminated, which librdkafka never gives. The second happens to a consumer that is " +
                "closed while a rebalance event is unserved: poll it until the rebalance is through before closing it." +
                (markedDisposed ? string.Empty : $" The consumer has no field '{DisposedField}' to mark it as disposed with, so disposing it may not return either."));
        }

        // Completed: rethrows what Close() threw, unwrapped.
        close.GetAwaiter().GetResult();
    }

    private static bool MarkDisposed<TKey, TValue>(IConsumer<TKey, TValue> consumer)
    {
        for (var type = consumer.GetType(); type is not null; type = type.BaseType)
        {
            var field = type.GetField(DisposedField, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
            if (field is not null && field.FieldType == typeof(bool))
            {
                field.SetValue(consumer, true);
                return true;
            }
        }

        return false;
    }
}
