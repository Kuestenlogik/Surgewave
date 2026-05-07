namespace Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;

/// <summary>
/// PostgreSQL wire protocol v3 frontend (client to server) message types.
/// </summary>
internal static class PgFrontendMessage
{
    /// <summary>Simple query ('Q').</summary>
    public const byte Query = (byte)'Q';

    /// <summary>Parse message for extended query protocol ('P').</summary>
    public const byte Parse = (byte)'P';

    /// <summary>Bind message for extended query protocol ('B').</summary>
    public const byte Bind = (byte)'B';

    /// <summary>Describe message for extended query protocol ('D').</summary>
    public const byte Describe = (byte)'D';

    /// <summary>Execute message for extended query protocol ('E').</summary>
    public const byte Execute = (byte)'E';

    /// <summary>Sync message — marks the end of an extended query cycle ('S').</summary>
    public const byte Sync = (byte)'S';

    /// <summary>Flush message ('H').</summary>
    public const byte Flush = (byte)'H';

    /// <summary>Close a named statement or portal ('C').</summary>
    public const byte Close = (byte)'C';

    /// <summary>Terminate the connection ('X').</summary>
    public const byte Terminate = (byte)'X';

    /// <summary>Password message ('p').</summary>
    public const byte Password = (byte)'p';
}

/// <summary>
/// PostgreSQL wire protocol v3 backend (server to client) message types.
/// </summary>
internal static class PgBackendMessage
{
    /// <summary>Authentication request ('R').</summary>
    public const byte Authentication = (byte)'R';

    /// <summary>Parameter status ('S').</summary>
    public const byte ParameterStatus = (byte)'S';

    /// <summary>Backend key data for cancel requests ('K').</summary>
    public const byte BackendKeyData = (byte)'K';

    /// <summary>Ready for query ('Z').</summary>
    public const byte ReadyForQuery = (byte)'Z';

    /// <summary>Row description ('T').</summary>
    public const byte RowDescription = (byte)'T';

    /// <summary>Data row ('D').</summary>
    public const byte DataRow = (byte)'D';

    /// <summary>Command complete ('C').</summary>
    public const byte CommandComplete = (byte)'C';

    /// <summary>Error response ('E').</summary>
    public const byte ErrorResponse = (byte)'E';

    /// <summary>Notice response ('N').</summary>
    public const byte NoticeResponse = (byte)'N';

    /// <summary>Empty query response ('I').</summary>
    public const byte EmptyQueryResponse = (byte)'I';

    /// <summary>Parse complete ('1').</summary>
    public const byte ParseComplete = (byte)'1';

    /// <summary>Bind complete ('2').</summary>
    public const byte BindComplete = (byte)'2';

    /// <summary>Close complete ('3').</summary>
    public const byte CloseComplete = (byte)'3';

    /// <summary>No data ('n').</summary>
    public const byte NoData = (byte)'n';

    /// <summary>Parameter description ('t').</summary>
    public const byte ParameterDescription = (byte)'t';
}

/// <summary>
/// Transaction status indicators for ReadyForQuery messages.
/// </summary>
internal static class PgTransactionStatus
{
    /// <summary>Idle (not in a transaction block).</summary>
    public const byte Idle = (byte)'I';

    /// <summary>In a transaction block.</summary>
    public const byte InTransaction = (byte)'T';

    /// <summary>In a failed transaction block.</summary>
    public const byte Failed = (byte)'E';
}
