# Interfaces Module — Core Contracts

Type-safe configuration and contract definitions for index operations. Enables compile-time verification of configuration and consistent option handling across all index submodules.

## Overview

Core contracts defining:
- Storage access abstraction (`ObjectStorageIndexAccessor`)
- Configuration options hierarchy
- Query request specifications
- Byte-range definitions

## Core Interfaces

### ObjectStorageIndexAccessor
**Primary abstraction** for storage operations

```java
public interface ObjectStorageIndexAccessor {
    InputStream getObject(String key, long byteStart, long byteEnd);
    List<String> listObjects(String prefix, String continuationToken);
    void putObject(String key, InputStream data);
}
```

**Implementations:**
- `AWSIndexAccess` — AWS S3
- `LocalIndexAccess` — Local filesystem
- Custom implementations for other backends

### ObjectStorageAccessor
**Lower-level** storage operations interface for raw object access

Implemented by storage-specific classes for byte-range support and connection management.

## Configuration Hierarchy

```
IndexContainerOptions (base)
    ├── ObjectStorageAccessOptions
    ├── IndexQueryOptions
    ├── IndexWriteOptions
    └── [Custom options]
```

### Option Classes

#### ObjectStorageAccessOptions
**Storage backend configuration**

```java
options
    .withAwsRegion("us-west-2")
    .withBucket("log-storage")
    .withEndpoint("https://s3.us-west-2.amazonaws.com")
    .withLocalPath("/var/log/storage")
```

**Common Properties:**
- `provider` — Storage backend type (aws, local, gcs, etc.)
- `region` — Cloud region (AWS)
- `bucket` — Storage container name
- `endpoint` — Custom endpoint URL

**AWS-Specific:**
- `accessKey` — AWS access key ID
- `secretKey` — AWS secret access key
- `sessionToken` — Temporary credential token

**Local-Specific:**
- `path` — Local filesystem path

#### IndexQueryOptions
**Query execution parameters**

```java
options
    .withStartTime(System.currentTimeMillis() - 3600000)
    .withEndTime(System.currentTimeMillis())
    .withTerms(Arrays.asList("ERROR", "TIMEOUT"))
    .withBloomFilterAccuracy(0.01)
    .withCacheTemplates(true)
```

**Properties:**
- `startTime` — Query start timestamp
- `endTime` — Query end timestamp
- `terms` — Search terms for filtering
- `bloomFilterAccuracy` — False positive rate (0.01 = 1%)
- `cacheTemplates` — Cache templates for repeated queries

#### IndexWriteOptions
**Index building configuration**

```java
options
    .withBloomFilterAccuracy(0.01)
    .withParallelThreads(4)
    .withBufferSize(65536)  // 64KB
```

**Properties:**
- `bloomFilterAccuracy` — Filter false positive rate
- `parallelThreads` — Worker threads for index building
- `bufferSize` — I/O buffer size in bytes

#### IndexFilterStats
**Statistics tracking** for index operations

Tracks:
- Filter construction time
- Bytes processed
- Elements added
- False positive counts (in testing)

## Query Specifications

### IndexQueryRequest
**Base query request** with common parameters

### QueryObjectRequest
**Single-object query** specification

### QueryObjectsOptions
**Multi-object query** parameters

## Byte-Range Definitions

### InputObjectByteRanges
**Specifies byte ranges** for partial object reads

```java
InputObjectByteRanges ranges = new InputObjectByteRanges();
ranges.add(0, 1000);        // First 1000 bytes
ranges.add(5000, 6000);     // Bytes 5000-6000
ranges.add(10000, -1);      // Bytes 10000 to end
```

## Usage Patterns

### Configuration Stacking
Build complex configurations using fluent API:

```java
ObjectStorageAccessOptions storage =
    new ObjectStorageAccessOptions()
        .withAwsRegion("us-west-2")
        .withBucket("logs");

IndexQueryOptions query =
    new IndexQueryOptions()
        .withTerms(Arrays.asList("ERROR"))
        .withBloomFilterAccuracy(0.001);

IndexQueryReader reader = new IndexQueryReader(
    accessor, query, storage);
```

### Type Safety
Configuration errors caught at compile time:

```java
// ❌ Compile error - method doesn't exist
options.withInvalidOption("foo");

// ✅ Type-safe
options.withAwsRegion("us-west-2");
```

## Extension

### Custom Options Class
```java
public class CustomIndexOptions extends IndexContainerOptions {
    private String customParam;
    
    public CustomIndexOptions withCustomParam(String param) {
        this.customParam = param;
        return this;
    }
}
```

### Factory Pattern
Factory methods select implementations based on options:

```java
ObjectStorageIndexAccessor accessor =
    IndexAccessorFactory.createAccessor(storageOptions);
```

## Design Principles

### Interface Segregation
- Separate concerns: storage, query, write operations
- Clients depend only on required interfaces
- Easy to mock for testing

### Builder Pattern
- Fluent configuration API
- Type-safe option validation
- Clear intent and readability

### Immutability
- Options objects typically immutable after construction
- Thread-safe to share across components
- Easier debugging and testing

## Testing

### Option Validation
```java
@Test
public void testInvalidRegion() {
    assertThrows(IllegalArgumentException.class, () ->
        new ObjectStorageAccessOptions()
            .withAwsRegion("")  // Invalid
    );
}
```

### Mock Implementations
```java
public class MockIndexAccessor implements ObjectStorageIndexAccessor {
    @Override
    public InputStream getObject(String key, long start, long end) {
        return new ByteArrayInputStream(data);
    }
}
```

## Dependencies

- Core Java interfaces and base classes
- No external dependencies
- Foundation for entire index module

## Related

- [Access Layer](../access/README.md) — Implementations
- [Query Module](../query/README.md) — Query operations
- [Write Module](../write/README.md) — Index building

---

Foundational contracts enabling extensible, type-safe index operations.
