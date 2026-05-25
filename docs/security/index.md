# Security Overview

Surgewave provides enterprise-grade security features.

## Security Layers

| Layer | Feature | Description |
|-------|---------|-------------|
| Authentication | [SASL](sasl.md) | Verify client identity |
| Encryption | [TLS](tls.md) | Encrypt data in transit |
| Authorization | [ACL](acl.md) | Control resource access |
| Supply chain | [Plugin Signing](plugin-signing.md) | Sign and verify `.swpkg` plugin packages end-to-end |

## Quick Setup

### Minimal Security

```json
{
  "Surgewave": {
    "Security": {
      "Enabled": true,
      "SaslEnabled": true,
      "SaslMechanisms": ["PLAIN"],
      "Users": [
        { "Username": "admin", "Password": "admin-secret" },
        { "Username": "app", "Password": "app-secret" }
      ]
    }
  }
}
```

### Full Security

```json
{
  "Surgewave": {
    "Security": {
      "Enabled": true,
      "SaslEnabled": true,
      "SaslMechanisms": ["SCRAM-SHA-256"],
      "TlsEnabled": true,
      "CertificatePath": "/certs/server.pfx",
      "CertificatePassword": "cert-password",
      "AclEnabled": true,
      "SuperUsers": ["User:admin"]
    }
  }
}
```

## Client Configuration

### .NET Producer

```csharp
var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.SecurityProtocol = SecurityProtocol.SaslSsl;
    options.SaslMechanism = SaslMechanism.ScramSha256;
    options.SaslUsername = "app";
    options.SaslPassword = "app-secret";
});
```

### Confluent.Kafka

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha256,
    SaslUsername = "app",
    SaslPassword = "app-secret"
};
```

## Security Protocols

| Protocol | SASL | TLS | Use Case |
|----------|------|-----|----------|
| PLAINTEXT | No | No | Development only |
| SASL_PLAINTEXT | Yes | No | Internal networks |
| SSL | No | Yes | Encryption only |
| SASL_SSL | Yes | Yes | Production |

## Best Practices

1. **Use SASL_SSL** in production
2. **Use SCRAM** over PLAIN
3. **Enable ACLs** for multi-tenant
4. **Rotate credentials** regularly
5. **Monitor auth failures**

## Next Steps

- [SASL Authentication](sasl.md) - Authentication mechanisms
- [TLS Encryption](tls.md) - Transport encryption
- [ACL Authorization](acl.md) - Access control
