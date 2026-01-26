# Index Module — Object Storage Querying

Production-grade object storage indexing system enabling efficient querying of log data stored in cloud storage (S3, local, etc.) with sub-second response times and minimal memory overhead.

## Overview

The Index module implements a complete indexing and query solution for cloud-native log storage:

- **Bloom Filter Indexes** — Probabilistic data structures for O(1) index pruning with <1% false positive rate
- **Byte-Range Optimization** — Partial file reads from cloud storage, eliminating unnecessary data transfer
- **Template Caching** — Pre-computed index templates for rapid query execution
- **Multi-Backend Support** — Seamless swapping between S3, local file system, and other storage providers
- **Scalable Query Execution** — Handles millions of events with minimal latency

## Architecture

### Component Layers

```
┌─────────────────────────────────────┐
│   Query Execution (query/)          │
│   IndexQueryReader, IndexQueryWriter │
├─────────────────────────────────────┤
│   Shared Foundations (shared/)      │
│   BaseIndexReader, BaseIndexWriter  │
├─────────────────────────────────────┤
│   Storage Abstraction (access/)     │
│   ObjectStorageIndexAccessor        │
├─────────────────────────────────────┤
│   Utilities (util/)                 │
│   BufferedLinesReader, ByteRange*   │
└─────────────────────────────────────┘
```

### Submodules

| Module | Classes | Purpose |
|--------|---------|---------|
| **[access/](./access/)** | 2 | S3 and local file system accessor implementations |
| **[filter/](./filter/)** | 5 | Bloom filter data structures and builders |
| **[interfaces/](./interfaces/)** | 9 | Core contracts and configuration options |
| **[query/](./query/)** | 10 | Query execution engine and filtering |
| **[shared/](./shared/)** | 8 | Base classes and factory patterns |
| **[util/](./util/)** | 13 | Stream utilities, buffering, filtering |
| **[write/](./write/)** | 9 | Index building and storage operations |

## Design Patterns

### Factory Pattern
`IndexAccessorFactory` abstracts storage backend selection:
```java
ObjectStorageIndexAccessor accessor =
    IndexAccessorFactory.createAccessor(options);
// Returns AWSIndexAccess for S3, LocalIndexAccess for filesystem
```

### Template Method
`BaseIndexReader` and `BaseIndexWriter` define index operation contracts with consistent resource management.

### Strategy Pattern
`ObjectStorageAccessor` enables different storage backends without changing client code.

### Decorator Pattern
Composable stream wrappers: `ByteRangeFilterInputStream`, `CompositeIndexReader`, `BufferedLinesReader`

## Performance Characteristics

### Bloom Filter Indexes
- **Time Complexity** — O(1) lookups
- **Space Complexity** — 10 bits per element for 1% false positive rate
- **Throughput** — 100k+ lookups/second per thread

### Byte-Range Queries
- **Latency** — Sub-100ms for typical queries from S3
- **Efficiency** — Only fetch relevant byte ranges from cloud storage

### Index Writing
- **Parallelization** — Multithreaded via ExecutorService
- **Throughput** — 50MB+/second per worker thread

## Key Classes

### Access Layer
- **`AWSIndexAccess`** — S3 with signed requests, byte-range support, list operations
- **`LocalIndexAccess`** — File system accessor for development

### Bloom Filters
- **`EncodedBloomFilter`** — Production filter with UTF8 support
- **`EncodedBloomFilterBuilder`** — Safe construction with configurable accuracy

### Query Execution
- **`IndexQueryReader`** — Composite reader for templates and search terms
- **`QueryFilterEvaluator`** — Evaluates filter expressions

### Utilities
- **`BufferedLinesReader`** — Line-by-line processing
- **`ByteRangeFileInputStream`** — File stream with byte-range support

## Usage Example

```java
// Initialize S3 accessor
ObjectStorageIndexAccessor accessor =
    IndexAccessorFactory.createAccessor(
        new ObjectStorageAccessOptions()
            .withAwsRegion("us-west-2"));

// Execute query
IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions()
        .withTerms(Arrays.asList("ERROR", "TIMEOUT")));

try (Reader results = reader.read()) {
    // Stream results
}
```

## Configuration

Index operations use option classes implementing `IndexContainerOptions`:

- **`ObjectStorageAccessOptions`** — Storage backend configuration
- **`IndexQueryOptions`** — Query parameters
- **`IndexWriteOptions`** — Index building settings
- **`IndexFilterStats`** — Statistics tracking

## Security

- **AWS S3** — SigV4 request signing, IAM role support
- **Data Privacy** — Bloom filters contain only hash values
- **Encryption** — Compatible with S3 encryption at rest
- **Multi-Cloud** — S3-compatible services supported

## Extending

### Add New Storage Backend
1. Implement `ObjectStorageAccessor`
2. Create `*IndexAccess` class extending base
3. Register in `IndexAccessorFactory`

### Custom Filters
Extend `CodedBloomFilter` for alternative implementations

## Testing

- Unit tests for individual components
- Integration tests with mock storage
- Performance tests for accuracy and throughput
- Cloud provider compatibility tests

## Dependencies

- AWS SDK for Java v2
- Jackson for JSON
- Java 11+

---

For detailed submodule documentation, see individual README files.
