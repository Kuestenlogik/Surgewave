# Shared Memory IPC

> **Enterprise Plugin**: Shared Memory transport is available as a separate plugin. See the Surgewave.Transport repository for documentation.

The Shared Memory transport provides ultra-low latency inter-process communication for co-located services using lock-free SPSC ring buffers. It is delivered as an enterprise .swpkg plugin that integrates with the Surgewave broker via the `IProtocolPlugin` interface.

## Installation

```bash
surgewave plugin install Kuestenlogik.Surgewave.Transport-x.y.z.swpkg
```
