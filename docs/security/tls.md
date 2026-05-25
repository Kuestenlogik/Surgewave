# TLS Encryption

Transport Layer Security for encrypted connections.

## Configuration

### Server

```json
{
  "Surgewave": {
    "Security": {
      "TlsEnabled": true,
      "CertificatePath": "/certs/server.pfx",
      "CertificatePassword": "cert-password",
      "ClientCertificateRequired": false
    }
  }
}
```

### With Client Authentication

```json
{
  "Surgewave": {
    "Security": {
      "TlsEnabled": true,
      "CertificatePath": "/certs/server.pfx",
      "CertificatePassword": "cert-password",
      "ClientCertificateRequired": true,
      "TrustedCertificatesPath": "/certs/ca.crt"
    }
  }
}
```

## Certificate Setup

### Generate Self-Signed (Development)

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt \
    -subj "/CN=Surgewave CA"

# Generate server certificate
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr \
    -subj "/CN=localhost"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt

# Create PFX
openssl pkcs12 -export -out server.pfx -inkey server.key -in server.crt \
    -certfile ca.crt -password pass:cert-password
```

### Production Certificates

Use certificates from:
- Let's Encrypt
- Internal PKI
- Commercial CA

## Client Configuration

### Surgewave.Client

TLS configuration is handled at the broker level. Native clients connect to the TLS-enabled port:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();
```

### Confluent.Kafka Compatibility Wrapper

For TLS with the Confluent.Kafka compatible API:

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.Ssl,
    SslCaLocation = "/certs/ca.crt",
    SslCertificateLocation = "/certs/client.crt",
    SslKeyLocation = "/certs/client.key"
};
```

### Confluent.Kafka

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.Ssl,
    SslCaLocation = "/certs/ca.crt"
};
```

## TLS + SASL

Combine encryption with authentication:

```json
{
  "Surgewave": {
    "Security": {
      "TlsEnabled": true,
      "SaslEnabled": true,
      "CertificatePath": "/certs/server.pfx",
      "SaslMechanisms": ["SCRAM-SHA-256"]
    }
  }
}
```

Client:

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha256,
    SaslUsername = "app",
    SaslPassword = "secret",
    SslCaLocation = "/certs/ca.crt"
};
```

## Certificate Rotation

1. Generate new certificate
2. Update server config
3. Restart broker (graceful)
4. Update clients

For zero-downtime:
- Configure multiple certificates
- Clients trust both old and new CA

## Troubleshooting

### Certificate Validation Failed

```
Error: SSL certificate verification failed
```

- Verify CA certificate is correct
- Check certificate chain
- Ensure CN matches hostname

### Handshake Failed

```
Error: SSL handshake failed
```

- Verify TLS versions match
- Check cipher suite compatibility
- Ensure certificate is valid

## Best Practices

1. **Use TLS 1.2+** - Disable older versions
2. **Strong ciphers** - Prefer ECDHE, AES-GCM
3. **Short validity** - 90 days for certificates
4. **Automate rotation** - Use cert-manager or ACME

## Next Steps

- [SASL](sasl.md) - Authentication
- [ACL](acl.md) - Authorization
