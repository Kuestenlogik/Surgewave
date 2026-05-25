# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**DO NOT** open a public GitHub issue for security vulnerabilities.

Instead, please report security vulnerabilities by emailing:

**security@kuestenlogik.de**

Or use GitHub's private vulnerability reporting feature if available.

### What to Include

Please include the following information in your report:

1. **Description**: A clear description of the vulnerability
2. **Impact**: The potential impact of the vulnerability
3. **Steps to Reproduce**: Detailed steps to reproduce the issue
4. **Affected Versions**: Which versions are affected
5. **Suggested Fix**: If you have suggestions for fixing the issue

### Response Timeline

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days
- **Fix Timeline**: Depends on severity
  - Critical: 24-72 hours
  - High: 1-2 weeks
  - Medium: 2-4 weeks
  - Low: Next release cycle

### Disclosure Policy

- We will acknowledge receipt of your report
- We will investigate and validate the issue
- We will work on a fix and coordinate disclosure
- We will credit you in the security advisory (unless you prefer anonymity)

## Scope

In scope:

- The `Kuestenlogik.Surgewave*` packages and the `surgewave` CLI tool
- The Surgewave broker (`Kuestenlogik.Surgewave.Broker`) and Control UI (`Kuestenlogik.Surgewave.Control`)
- The Kafka wire-protocol implementation (`Kuestenlogik.Surgewave.Protocol.Kafka`) and other built-in protocols
- The plugin install / load surface (`surgewave plugin install`, ALC isolation, `.swpkg` package handling)
- The published OCI container image at `ghcr.io/kuestenlogik/surgewave`
- The release artefacts (MSI / DEB / RPM / Homebrew / winget)

Out of scope:

- Third-party plugins published outside the `Kuestenlogik/` organisation
- Bugs in upstream dependencies (please report those to the upstream project; we will track and consume the fix)
- Findings that require an attacker to already have local code execution on the host running the broker
- Self-inflicted misconfiguration (e.g. exposing `9092` to the public internet without SASL/TLS)

## Security Best Practices

When deploying Surgewave in production:

### Network Security

- Use TLS for all client-broker communication
- Enable mTLS for broker-to-broker communication
- Use network segmentation to isolate broker traffic
- Configure firewalls to restrict access

### Authentication

- Enable SASL authentication
- Use strong passwords or certificate-based auth
- Rotate credentials regularly
- Use SCRAM-SHA-512 for password-based auth

### Authorization

- Enable ACLs in production
- Follow principle of least privilege
- Regularly audit access permissions
- Use separate credentials for different applications

### Encryption

- Enable encryption in transit (TLS 1.2+)
- Consider encryption at rest for sensitive data
- Use strong cipher suites

### Monitoring

- Monitor for unusual access patterns
- Set up alerts for authentication failures
- Log all administrative actions
- Regularly review audit logs

## Security Updates

Security updates will be released as patch versions and announced via:
- GitHub Security Advisories
- Release notes
- Project mailing list (when available)

Always run the latest patch version to ensure you have the latest security fixes.
