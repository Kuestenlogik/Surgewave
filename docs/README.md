# Surgewave Documentation

Full documentation for Surgewave.

## Contents

| Document | Description |
|----------|-------------|
| [API Reference](api/index.md) | Generated API documentation |
| [Configuration](setup/configuration.md) | Broker and client configuration |
| [Deployment](deployment/index.md) | Production deployment guides |
| [Quickstart](quickstart/index.md) | Getting started guide |

## Quick Links

- [Getting Started](quickstart/index.md)
- [Architecture](setup/architecture.md)
- [Contributing](https://github.com/Kuestenlogik/Surgewave/blob/main/CONTRIBUTING.md)
- [Changelog](https://github.com/Kuestenlogik/Surgewave/blob/main/CHANGELOG.md)

## Building Documentation

```powershell
# Build the documentation site
.\scripts\docs.ps1

# Build and serve at http://localhost:8080
.\scripts\docs.ps1 -Serve

# Regenerate API metadata from source first
.\scripts\docs.ps1 -Clean -WithMetadata
```

Documentation is built using DocFX and outputs to `artifacts/docs/`.

## Additional Resources

- **Benchmarks**: See [benchmarks README](https://github.com/Kuestenlogik/Surgewave/tree/main/benchmarks)
- **Examples**: See [examples README](https://github.com/Kuestenlogik/Surgewave/tree/main/examples)
- **Tests**: See [tests README](https://github.com/Kuestenlogik/Surgewave/tree/main/tests)
