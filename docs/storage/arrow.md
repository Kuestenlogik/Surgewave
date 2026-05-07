# Apache Arrow Storage

> **Enterprise Plugin**: Apache Arrow storage is available as a separate plugin. See the Surgewave.Storage.Arrow repository for documentation.

The Arrow storage engine provides columnar storage optimized for analytics workloads, with zero-copy memory-mapped reads and direct compatibility with analytics tools. It is delivered as an enterprise .swpkg plugin that integrates with the Surgewave broker via the `IStorageEnginePlugin` interface.

## Installation

```bash
surgewave plugin install Kuestenlogik.Surgewave.Storage.Arrow-x.y.z.swpkg
```
