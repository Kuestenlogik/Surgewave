namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Exception thrown when SQL parsing fails.
/// </summary>
public sealed class SqlParseException : Exception
{
    public SqlParseException() { }
    public SqlParseException(string message) : base(message) { }
    public SqlParseException(string message, Exception inner) : base(message, inner) { }
}
