# Cloud Extensions

Production-grade cloud integrations for the Log10x pipeline engine. Provides enterprise-ready abstractions for cloud storage indexing, stream processing, metrics publishing, and cloud provider integrations.

## Overview

Cloud Extensions extends the core Log10x pipeline with specialized capabilities for cloud-native deployments:

- **Object Storage Indexing** — Efficient querying of data stored in cloud object storage (S3, etc.) using bloom filters and byte-range optimization
- **Stream Processing** — Apache Camel-based input processors for analyzer streams with authentication and signing support
- **Metrics Publishing** — Time-series output to major monitoring backends (CloudWatch, Datadog, Elastic, SignalFx)
- **Cloud Integrations** — GitHub output, AWS request signing, secure HTTP authentication

## Architecture

```
cloud-extensions/
├── index/              (57 classes) - Object storage indexing & querying
├── camel/             (9 classes)  - Apache Camel stream processors
├── micrometer/        (4 classes)  - Time-series metric backends
├── compile/           (1 class)    - Symbol extraction schema
└── github/            (1 class)    - GitHub output integration
```

### Design Patterns

- **Factory Pattern** — `IndexAccessorFactory`, `MetricRegistryFactory` for provider abstraction
- **Template Method** — `BaseIndexReader`, `BaseIndexWriter`, `BaseRouteProcessor` for consistent behavior
- **Strategy Pattern** — `ObjectStorageAccessor` implementations (AWS S3, Local)
- **Decorator Pattern** — `ByteRangeFilterInputStream`, `CompositeIndexReader` for composable functionality
- **Builder Pattern** — `EncodedBloomFilterBuilder` for complex object construction

## Module Breakdown

| Module | Purpose | Classes |
|--------|---------|---------|
| **[index/](./index/)** | Object storage querying with bloom filters and byte-range optimization | 57 |
| **[camel/](./camel/)** | Apache Camel processors for input stream handling | 9 |
| **[micrometer/registry/](./micrometer/registry/)** | Metric registry factories for 4 monitoring backends | 4 |
| **[compile/symbol/](./compile/symbol/)** | Symbol extraction schema and utilities | 1 |
| **[github/](./github/)** | GitHub repository output integration | 1 |

## Enterprise Features

### Performance & Scalability
- **Bloom Filters** — O(1) probabilistic lookups for index pruning, <1% false positive rate
- **Byte-Range I/O** — Partial file reads from cloud storage, no unnecessary data transfer
- **Parallel Processing** — Multithreaded index building with `ExecutorService` and synchronization primitives
- **Lazy Initialization** — Deferred stream creation for resource efficiency
- **Buffered I/O** — Configurable buffer sizes (8KB default) for optimal throughput

### Security & Authentication
- **AWS Signature V4** — Request signing for authenticated S3 access via `AwsSignatureProcessor`
- **HTTP Auth** — Pluggable authentication for HTTP-based inputs
- **GitHub OAuth** — Token-based GitHub integration via `GitHubOutputStream`
- **Flexible SSL** — Configurable SSL context for different security requirements

### Reliability & Maintainability
- **Clear Abstraction Layers** — `ObjectStorageIndexAccessor` interface allows S3/Local/GCS swapping without client changes
- **Consistent Error Handling** — Standard exception patterns across all modules
- **Comprehensive Filtering** — Single-term, multi-term, and custom filter implementations
- **Type Safety** — Generic implementations (e.g., `CamelStreamProcessor<T>`) for compile-time checking

## Quick Links

- **Getting Started**: See [index/README.md](./index/README.md) for indexing examples
- **API Reference**: Explore [index/interfaces/README.md](./index/interfaces/README.md) for contracts
- **Integration Guide**: Check [camel/README.md](./camel/README.md) for Camel processors
- **Monitoring**: Review [micrometer/registry/README.md](./micrometer/registry/README.md) for metrics backends

## Dependencies

- **Apache Camel 3.x** — Stream routing and processing
- **Micrometer** — Application metrics abstractions
- **AWS SDK for Java** — S3 and CloudWatch clients
- **Kohsuke GitHub Library** — GitHub API integration
- **Jackson** — JSON serialization/deserialization

## Development Guidelines

### Adding New Storage Backends
1. Implement `ObjectStorageAccessor` interface
2. Create `*IndexAccess` class extending `BaseIndexAccess`
3. Register in `IndexAccessorFactory.createAccessor()`
4. Add integration tests in test module

### Adding New Metric Backends
1. Create `*MetricRegistryFactory` extending `MetricRegistryFactory`
2. Implement registry creation using Micrometer libraries
3. Register in factory loader

### Extending Stream Processors
1. Extend `CamelStreamProcessor<T>` or `BaseRouteProcessor`
2. Implement required stream output creation
3. Use in `EventCamelStreamRouteBuilder` route definitions

## Testing & Quality

- All public APIs have corresponding integration tests
- Performance testing of bloom filters under various configurations
- Security testing of authentication and signing mechanisms
- Compatibility testing with major cloud providers (AWS, GCP, Azure)

## License

Copyright Log10x. All rights reserved.

---

For detailed module documentation, see individual README files in subdirectories.
