using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Hands a response lease to the request that is waiting for it — or releases it when nobody is.
///
/// <para><b>Why this is shared rather than written twice.</b> Both transports run the same reader
/// loop and both must get the same ownership rule right: a lease holds a pooled buffer, so every
/// branch needs an owner. The pending-request map is the arbiter — whoever removes the entry owns
/// the completion — and the loser is left holding a lease nobody will consume. Duplicated in two
/// files, a fix applied to one leaks buffers in the other, and the QUIC copy has no tests of its
/// own (#80/#117).</para>
/// </summary>
public static class ResponseDelivery
{
    /// <summary>
    /// Delivers <paramref name="lease"/> to the waiter registered under <paramref name="requestId"/>,
    /// or disposes it if there is none — because the request was cancelled and its waiter already
    /// took the entry, or because the waiter lost the completion race.
    /// </summary>
    /// <returns><see langword="true"/> if a waiter received it.</returns>
    public static bool DeliverOrRelease(
        ConcurrentDictionary<uint, PendingResponse> pendingRequests,
        uint requestId,
        SurgewaveResponseLease lease)
    {
        if (pendingRequests.TryRemove(requestId, out var pending) && pending.TrySetResult(lease))
        {
            // Recycling happens on the awaiting side once the result is consumed: returning the
            // instance here would hand it out again while the waiter still holds its token.
            return true;
        }

        lease.Dispose();
        return false;
    }
}
