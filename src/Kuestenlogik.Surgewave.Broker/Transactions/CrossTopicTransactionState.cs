namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// State of a cross-topic transaction lifecycle.
/// </summary>
public enum CrossTopicTransactionState
{
    /// <summary>Transaction is open and accepting writes.</summary>
    Open,

    /// <summary>Transaction is in the process of committing (two-phase commit phase 1).</summary>
    Committing,

    /// <summary>Transaction has been successfully committed.</summary>
    Committed,

    /// <summary>Transaction is in the process of aborting.</summary>
    Aborting,

    /// <summary>Transaction has been aborted.</summary>
    Aborted,

    /// <summary>Transaction exceeded its timeout and was auto-aborted.</summary>
    TimedOut
}
