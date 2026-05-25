namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Isolation level for transactional reads.
/// </summary>
public enum IsolationLevel
{
    /// <summary>
    /// Read all messages, including uncommitted transactional messages.
    /// </summary>
    ReadUncommitted,

    /// <summary>
    /// Only read committed transactional messages.
    /// </summary>
    ReadCommitted
}
