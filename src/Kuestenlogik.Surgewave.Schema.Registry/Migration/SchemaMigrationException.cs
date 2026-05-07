namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Exception thrown when a schema migration fails due to incompatible data or configuration.
/// </summary>
public sealed class SchemaMigrationException : Exception
{
    /// <summary>
    /// Creates a new <see cref="SchemaMigrationException"/> with the specified message.
    /// </summary>
    public SchemaMigrationException(string message) : base(message) { }

    /// <summary>
    /// Creates a new <see cref="SchemaMigrationException"/> with the specified message and inner exception.
    /// </summary>
    public SchemaMigrationException(string message, Exception innerException) : base(message, innerException) { }
}
