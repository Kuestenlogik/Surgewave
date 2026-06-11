namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// A single entry in a disaggregated partition manifest: one object in
/// the object store that holds a contiguous offset range. The manifest
/// for a partition is the ordered list of these refs; reads union the
/// local-WAL tail (if any) with the refs covering the requested range.
/// </summary>
/// <param name="ObjectKey">
/// Opaque key in the object store (e.g. an S3 key like
/// <c>topics/orders/0/stream-0000000-0099999.so</c>). Format is
/// engine-internal; clients never see it.
/// </param>
/// <param name="FirstOffset">First message offset contained in this object (inclusive).</param>
/// <param name="LastOffset">Last message offset contained in this object (inclusive).</param>
/// <param name="BytesOnDisk">Size of the object payload in bytes. Used for cost reporting + cache sizing.</param>
/// <param name="CreatedAt">UTC timestamp when the object was PUT. Used for retention enforcement.</param>
public readonly record struct StreamObjectRef(
    string ObjectKey,
    long FirstOffset,
    long LastOffset,
    long BytesOnDisk,
    DateTime CreatedAt)
{
    /// <summary>Number of messages this object contains.</summary>
    public long RecordCount => LastOffset - FirstOffset + 1;

    /// <summary>Whether the requested offset falls inside this object's range.</summary>
    public bool Contains(long offset) => offset >= FirstOffset && offset <= LastOffset;
}
