namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// Protocol-neutral audit seam — the topic/security-event surface the relocatable Kafka
/// handlers (TopicAdmin / Security) record on. The concrete <c>AuditLogger</c> stays in the
/// broker host; the plugin depends only on this interface (#59 b5). Optional: injected as
/// <c>null</c> when auditing is off.
/// </summary>
public interface IAuditLogger
{
    /// <summary>Records a topic/security lifecycle event to the audit sink.</summary>
    void LogTopicEvent(
        AuditEventType eventType,
        string topicName,
        string? principal,
        string? clientAddress,
        string? clientId,
        bool success = true,
        string? errorMessage = null,
        Dictionary<string, string>? details = null);

    /// <summary>Records an ACL create/delete event (Kafka Security handler, KIP-11).</summary>
    void LogAclEvent(
        AuditEventType eventType,
        string resourceType,
        string resourceName,
        string? principal,
        string? clientAddress,
        bool success = true,
        string? errorMessage = null,
        Dictionary<string, string>? details = null);

    /// <summary>Records a SASL authentication attempt/result (Kafka Security handler).</summary>
    void LogAuthenticationEvent(
        AuditEventType eventType,
        string? principal,
        string? clientAddress,
        string? mechanism,
        bool success = true,
        string? errorMessage = null);
}
