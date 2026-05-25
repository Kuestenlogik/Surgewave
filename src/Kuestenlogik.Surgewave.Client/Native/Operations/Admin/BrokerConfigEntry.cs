namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Broker configuration entry.
/// </summary>
public record BrokerConfigEntry(string Name, string Value, bool IsReadOnly, bool IsDefault, bool IsSensitive);
